# coding: utf-8
#!/usr/bin/env python2.7
# gtfs.py

"""GTFS Utility & CRUD class pertaining to a google transit feed.
It can be used to create, update, remove to/from a datastore (via sqlalchemy).
"""

import config
import urllib2
import time
import os
import zipfile
import csv
import shutil
from datastore import Management, Feeds
from optparse import OptionParser


class GTFS():
    def __init__(self):
        """This class contains all the CRUD methods needed to create/update/remove a gtfs feed"""
        # if the databases do not exist, create them
        self.management = Management()
        self.feeds = Feeds()
        
        # grab db connections
        self.conn_mngmt = self.management.get_connection()
        self.conn_feeds = self.feeds.get_connection()
        
        # grab table objects (management)
        self.tables_map = {
            'feed':             self.management.get_feed_table(),
            'dataset':          self.management.get_dataset_table(),
            # grab table objects (feeds) into a tables_map dictionary, in which 
            # its keys will be used (tables_map.keys()) as a list of expected tables
            'agency':           self.feeds.get_agency_table(),
            'stops':            self.feeds.get_stops_table(),
            'routes':           self.feeds.get_routes_table(),
            'trips':            self.feeds.get_trips_table(),
            'stop_times':       self.feeds.get_stop_times_table(),
            'calendar':         self.feeds.get_calendar_table(),
            'calendar_dates':   self.feeds.get_calendar_dates_table(),
            'fare_attributes':  self.feeds.get_fare_attributes_table(),
            'fare_rules':       self.feeds.get_fare_rules_table(),
            'shapes':           self.feeds.get_shapes_table(),
            'frequencies':      self.feeds.get_frequencies_table(),
            'transfers':        self.feeds.get_transfers_table(),
            'feed_info':        self.feeds.get_feed_info_table(),
        }
        
        # list of column names per table
        self.gtfs_columns = {
            'agency': ['agency_id', 'agency_name', 'agency_url', 'agency_timezone', 'agency_lang', 'agency_phone', 'agency_fare_url'],
            'stops': ['stop_id', 'stop_code', 'stop_name', 'stop_desc', 'stop_lat', 'stop_lon', 'zone_id', 'stop_url', 'location_type', 'parent_station', 'stop_timezone'],
            'routes': ['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_desc', 'route_type', 'route_url', 'route_color', 'route_text_color'],
            'trips': ['route_id', 'service_id', 'trip_id', 'trip_headsign', 'trip_short_name', 'direction_id', 'block_id', 'shape_id'],
            'stop_times': ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence', 'stop_headsign', 'pickup_type', 'drop_off_type', 'shape_dist_traveled'],
            'calendar': ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date'],
            'calendar_dates': ['service_id', 'date', 'exception_type'],
            'fare_attributes': ['fare_id', 'price', 'currency_type', 'payment_method', 'transfers', 'transfer_duration'],
            'fare_rules': ['fare_id', 'route_id', 'origin_id', 'destination_id', 'contains_id'],
            'shapes': ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence', 'shape_dist_traveled'],
            'frequencies': ['trip_id', 'start_time', 'end_time', 'headway_secs', 'exact_times'],
            'transfers': ['from_stop_id', 'to_stop_id', 'transfer_type', 'min_transfer_time'],
            'feed_info': ['feed_publisher_name', 'feed_publisher_url', 'feed_lang', 'feed_start_date', 'feed_end_date', 'feed_version']
        }
        
    def create(self, feed_name, feed_url, feed_country, feed_city, feed_agency, feed_timezone):
        """Creates a new feed into the datastore. Returns True is successful or an 
        error message string if it failed.
        Usage: object.create( feed_name="canada_hamilton_hsr", feed_url="http://feed.zip",
                            feed_country="Canada", feed_city="Hamilton", feed_agency="HSR",
                            feed_timezone="America/Toronto" )
        """
        
        # exception handling for errors
        result = None
        try:
            # create dataset properties
            self.feed_name = feed_name.lower()
            self.feed_url = feed_url
            self.feed_country = feed_country
            self.feed_city = feed_city
            self.feed_agency = feed_agency
            self.feed_timezone = feed_timezone
            # create the unique dataset_id from the feed_name and current system's timestamp
            self.dataset_id = self.feed_name + "_" + str( int(time.time()) )
            
            # validate the uniqueness of feed_name, needs to be done before a feed is registered..
            feed_record = self._find_feed_record(feed_name)
            if feed_record:
                # the feed exists, return an error to the caller (raise GTFSError exception)
                raise GTFSError("Feed: " + feed_name + " already exists. Retry with a UNIQUE feed name OR use the -u option on " + feed_name + " to update it.")
            
            # validate url, needs to be done before a feed is registered..
            self._validate_feed_url(feed_url)
            
            # register the new feed with the management database
            self.conn_mngmt.execute(self.tables_map['feed'].insert(), [
                {"feed_name": self.feed_name, "feed_url": self.feed_url, "feed_country": self.feed_country, "feed_city": self.feed_city, "feed_agency": self.feed_agency, "feed_timezone": self.feed_timezone}
            ])
            
            # create the dataset
            self._create_dateset()
            print('created')
            
        except GTFSError, err:
            result = err # GTFS custom exceptions only contain a string
        except Exception, err:
            result = str(err)
        else:
            result = True;
        return result
        
    def _create_dateset(self):
        # creates the dataset (download, parse, insert feed) and links it to the feed, then does the cleanup
        # download the zip file
        self._download_file()
        
        # unzip the file
        self._unzip_file()
        
        # populate the feeds table
        self._populate_feeds()
        
        # register dataset and link it to its corresponding feed
        self._register_dataset()
        
        # clean up mess (temp files, database tables verification records, etc..)
        self._cleanup()
        
    def _download_file(self):
        # downloads the zip file into the 'tmp' directory in the current directory
        root_dir = os.path.dirname(os.path.realpath(__file__))
        # create the path to the 'tmp' directory, and if it doesn't exist, create it
        self.tmp_dir = os.path.join(root_dir, 'tmp/')
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        # create the path to the dataset directory and also create the path to the zip file to be downloaded (zip file's name will be the same as the dataset_id)
        self.dataset_dir = os.path.join(self.tmp_dir, self.dataset_id)
        self.dataset_zipfile = os.path.join(self.dataset_dir + '.zip') # filename = dirname+'.zip'
        
        # process the zip file download http request, then write the file locally with the name self.dataset_zipfile
        request = urllib2.urlopen(self.feed_url)
        with open(self.dataset_zipfile, 'wb') as fp:
            while 1:
                packet = request.read()
                if not packet:
                    break
                fp.write(packet)
        self.last_modified =  request.info()['Last-Modified']
        request.close()
        
    def _unzip_file(self):
        # method unzips the downloaed file into self.dataset_dir
        os.mkdir(self.dataset_dir, 0777)
        # unzip the files into the newly created dir
        zip = zipfile.ZipFile(self.dataset_zipfile)
        for name in zip.namelist():
            # if name is a directory, create it
            if name.endswith('/'):
                os.mkdir(os.path.join(self.dataset_dir, name))
            else:
                # write all filenames as lowercase.
                handle = open(os.path.join(self.dataset_dir, name.lower()), 'wb')
                handle.write(zip.read(name.lower()))
                handle.close()
        
    def _populate_feeds(self):
        # read files and populate the feeds database
        # make sure the file exists, before populating.
        for table in self.tables_map.keys():
            file_path = os.path.join(self.dataset_dir, table + '.txt')
            # the file exists, read and populate
            if os.path.exists(file_path):
                self.headers = None
                self.bulk_insert = []
                self.exclusions = []
                with open(file_path, 'rb') as handle:
                    reader = csv.reader(handle)
                    for row in reader:
                        # determine which header columns should be removed (extra, not in gtfs)
                        if not self.headers:
                            self.headers = row
                        else:
                            # add a dict element made from row into self.bulk_insert list, which will be shoved into sqlalchemy's insert block
                            self._add_to_bulk_insert(row, table)
                # send to datastore
                self._send_to_datastore(table)
            else:
                # the file does NOT exist, continue
                continue
        
    def _add_to_bulk_insert(self, row_list, table_name):
        # creates a new dict element out of self.headers, self.exclusions, and row_list, then adds it to self.bulk_insert
        # It also removes columns (and all their associated values) from the file that are not part of the gtfs feed (extra columns)
        # It also sets None values in-place of missing values in rows (even if their corresponding columns are present)
        row_dict = {}
        column_index = 0
        # only insert the columns that we have in the files, others will default to null in the datastore
        for column in self.headers:
            # if this column is an extra/excess (not defined in gtfs feed), skip it, BUT the column_index MUST be incremented either way
            if column in self.gtfs_columns[table_name]:
                # exception handling for when we try to access a list's index that doesn't exist (missing values when columns are present)
                # if an exception is raised, then the value for the corresponding column is missing, set it to None (null) for the datastore's insert stmt
                try:
                    # don't insert empty string, convert them to None (null in datastore)
                    if row_list[column_index]:
                        row_dict[column] = row_list[column_index]
                    else:
                        row_dict[column] = None
                except IndexError, e:
                    row_dict[column] = None
            # increment the column index either way
            column_index = column_index + 1
            # add the dataset_id column if row_dict has any values
            if row_dict:
                row_dict['dataset_id'] = self.dataset_id
        # add the clean row's dictionary to the list to insert
        self.bulk_insert.append(row_dict)
        
    def _send_to_datastore(self, table_name):
        # if bulk_insert dict contains values, then insert them
        if self.bulk_insert:
            self.conn_feeds.execute(self.tables_map[table_name].insert(), self.bulk_insert)
        
    def _register_dataset(self):
        # register the dataset in the management database (dataset table), registration = mission accomplished
        self.conn_mngmt.execute(self.tables_map['dataset'].insert(), [
            {"dataset_id": self.dataset_id, "feed_name": self.feed_name, "last_modified": self.last_modified}
        ])
        
    def _cleanup(self):
        # clean up mess (temp files, database tables verification records, etc..)
        # for now, remove dataset's temp files (zipfile, dataset directory - including the extracted files within)
        shutil.rmtree(self.dataset_dir)
        os.remove(self.dataset_zipfile)
        
    def _find_feed_record(self, feed_name):
        # return the feed record that has feed_name = feed_name
        # select from management.feed where feed_name = feed_name
        query = self.tables_map['feed'].select()
        # add a where predicate
        query = query.where(
            self.tables_map['feed'].c.feed_name == feed_name
        )
        # get the result set
        result = self.conn_mngmt.execute(query).fetchone()
        
        # simply return the record (code re-use create and update methods :)
        return result
        
    def _validate_feed_url(self, feed_url):
        # make sure the feed url is valid
        try:
            handle = urllib2.urlopen(urllib2.Request(feed_url))
        except urllib2.URLError, e:
            raise GTFSError("URLError: " + str(e.reason))
        except urllib2.HTTPError, e:
            raise GTFSError("HTTPError: " + str(e.code) + " while trying to reach " + feed_url)
        
    def update(self, feed_name):
        """Updates a current specific feed in the datastore.
        Usage: object.update( feed_name="canada_hamilton_hsr" )
        """
        
        # exception handling for errors
        result = None
        try:
            # create dataset properties
            self.feed_name = feed_name.lower()
            # create the unique dataset_id from the feed_name and current system's timestamp
            self.dataset_id = self.feed_name + "_" + str( int(time.time()) )
            
            # find the record with feed_name = feed_name
            feed_record = self._find_feed_record(feed_name)
            if feed_record:
                # an elegant way to create a dict out of a result set list while not hardcoding column names (for future)
                column_list = [col.name for col in self.tables_map['feed'].columns]
                row_dict = dict(zip(column_list, feed_record))
                # set the dataset properties
                self.feed_url = row_dict['feed_url']
                self.feed_country = row_dict['feed_country']
                self.feed_city = row_dict['feed_city']
                self.feed_agency = row_dict['feed_agency']
                self.feed_timezone = row_dict['feed_timezone']
            else:
                # record does not exist, how can I update something that doesn't exist?? PUNISH THE USER!!!
                raise GTFSError("Feed: " + feed_name + " does not exist. Are you trolling?")
            
            # create the dataset
            self._create_dateset()
            
            # TODO: remove older datasets..
            print('updated')
            
        except GTFSError, err:
            result = err # GTFS custom exceptions only contain a string
        except Exception, err:
            result = str(err)
        else:
            result = True;
        return result
        
    def remove(self, feed_name):
        """Removes a current specific feed from the datastore.
        Usage: object.remove( feed_name="canada_hamilton_hsr" )
        """
        
        # exception handling for errors
        result = None
        try:
            print('remove - coming soon')
            
        except GTFSError, err:
            result = err # GTFS custom exceptions only contain a string
        except Exception, err:
            result = str(err)
        else:
            result = True;
        return result
        

def main():
    """Entrypoint of the gtfs script (through the command-line)"""
    
    # Use python's OptionParser class to handle command-line arguments
    usage = "\n\n" + "python gtfs.py -c feed_name http://feed_url.zip country city agency timezone" + "\n" + "python gtfs.py -u feed_name" + "\n" + "python gtfs.py -r feed_name"
    parser = OptionParser(usage=usage, version="%prog " + str(config.VERSION))
    
    # add the create option
    # usage: "python gtfs.py -c canada_hamilton_hsr http://googlehsrdocs.hamilton.ca Canada Hamilton HSR America/Toronto"
    parser.add_option("-c", dest="feed_name_to_create", action="store",
                      help="create a new feed into the datastore")
    # add the update option
    # usage: "python gtfs.py -u canada_hamilton_hsr"
    parser.add_option("-u", dest="feed_name_to_update", action="store",
                      help="update a current feed in the datastore")
    # add the remove/delete option
    # usage: "python gtfs.py -r canada_hamilton_hsr"
    parser.add_option("-r", dest="feed_name_to_remove", action="store",
                      help="remove a current feed from the datastore")
    
    # assign the options (dest) dictionary into options, and the arguments list into args
    (options, args) = parser.parse_args()
    
    # transfer control to the targetted function (create, update, remove) based on the command-line options
    if options.feed_name_to_create is not None:
        # check the number of arguments (makes sure the user supplied the right amount)
        if len(args) != 5:
            parser.error('Insufficient arguments. Use gtfs.py --help for a list of valid options')
        # execute the create function
        feed_name = options.feed_name_to_create
        feed_url = args[0]
        feed_country = args[1]
        feed_city = args[2]
        feed_agency = args[3]
        feed_timezone = args[4]
        # create the feed
        feed = GTFS()
        result = feed.create(feed_name, feed_url, feed_country, feed_city, feed_agency, feed_timezone)
        # return the error message if the operation failed
        if not isinstance(result, bool):
            parser.error(result)
        
    elif options.feed_name_to_update is not None:
        # update the feed
        feed = GTFS()
        result = feed.update(options.feed_name_to_update)
        # return the error message if the operation failed
        if not isinstance(result, bool):
            parser.error(result)
        
    elif options.feed_name_to_remove is not None:
        # remove the feed
        feed = GTFS()
        result = feed.remove(options.feed_name_to_remove)
        # return the error message if the operation failed
        if not isinstance(result, bool):
            parser.error(result)
        
    else:
        # the user didn't supply the correct options in the script
        parser.error('Invalid command-line option. Use gtfs.py --help for a list of valid options')
    

class GTFSError(Exception):
    pass
    

if __name__ == '__main__':
    # run main()
    main()
else:
    # module added
    print('gtfs module is loaded.')
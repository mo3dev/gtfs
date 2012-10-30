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
        self.feed_table     = self.management.get_feed_table()
        self.dataset_table  = self.management.get_dataset_table()
        # grab table objects (feeds)
        self.agency_table           = self.feeds.get_agency_table()
        self.stops_table            = self.feeds.get_stops_table()
        self.routes_table           = self.feeds.get_routes_table()
        self.trips_table            = self.feeds.get_trips_table()
        self.stop_times_table       = self.feeds.get_stop_times_table()
        self.calendar_table         = self.feeds.get_calendar_table()
        self.calendar_dates_table   = self.feeds.get_calendar_dates_table()
        self.fare_attributes_table  = self.feeds.get_fare_attributes_table()
        self.fare_rules_table       = self.feeds.get_fare_rules_table()
        self.shapes_table           = self.feeds.get_shapes_table()
        self.frequencies_table      = self.feeds.get_frequencies_table()
        self.transfers_table        = self.feeds.get_transfers_table()
        self.feed_info_table        = self.feeds.get_feed_info_table()
        
        # list of expected tables
        self.gtfs_tables = [
            'agency', 'stops', 'routes', 'trips', 'stop_times', 'calendar',
            'calendar_dates', 'fare_attributes', 'fare_rules', 'shapes',
            'frequencies', 'transfers', 'feed_info'
        ]
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
        print("creating " + feed_name)
        
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
            
            # validate url, needs to be done before a feed is registered..
            self._validate_feed_url(feed_url)
            
            # register the new feed with the management database
            self.conn_mngmt.execute(self.feed_table.insert(), [
                {"feed_name": self.feed_name, "feed_url": self.feed_url, "feed_country": self.feed_country, "feed_city": self.feed_city, "feed_agency": self.feed_agency, "feed_timezone": self.feed_timezone}
            ])
            
            # create the dataset
            self._create_dateset()
            
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
        
    def _download_file(self):
        # downloads the zip file into the 'tmp' directory in the current directory
        root_dir = os.path.dirname(os.path.realpath(__file__))
        # create the path to the 'tmp' directory, and if it doesn't exist, create it
        self.tmp_dir = os.path.join(root_dir, 'tmp/')
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        # create the path to the dataset directory and also create the path to the zip file to be downloaded (zip file's name will be the same as the dataset_id)
        self.dataset_dir = os.path.join(self.tmp_dir, self.dataset_id)
        self.target_file = os.path.join(self.dataset_dir + '.zip') # filename = dirname+'.zip'
        
        # process the zip file download http request, then write the file locally with the name self.target_file
        request = urllib2.urlopen(self.feed_url)
        with open(self.target_file, 'wb') as fp:
            while 1:
                packet = request.read()
                if not packet:
                    break
                fp.write(packet)
        self.date_last_modified =  request.info()['Last-Modified']
        request.close()
        print('downloaded')
        
    def _unzip_file(self):
        # method unzips the downloaed file into self.dataset_dir
        os.mkdir(self.dataset_dir, 0777)
        # unzip the files into the newly created dir
        zip = zipfile.ZipFile(self.target_file)
        for name in zip.namelist():
            # if name is a directory, create it
            if name.endswith('/'):
                os.mkdir(os.path.join(self.dataset_dir, name))
            else:
                # write all filenames as lowercase.
                handle = open(os.path.join(self.dataset_dir, name.lower()), 'wb')
                handle.write(zip.read(name.lower()))
                handle.close()
        print('unzipped')
        
    def _populate_feeds(self):
        # read files and populate the feeds database
        # make sure the file exists, before populating.
        for table in self.gtfs_tables:
            file_path = os.path.join(self.dataset_dir, table + '.txt')
            if os.path.exists(file_path):
                # the file exists, read and populate
                handle = open(file_path, 'r')
                # loop through the file (efficiently)
                header = False
                for line in handle:
                    # first line is header (columns)
                    if not header:
                        print line, # header
                        header = True # deactivate the flag
                    else:
                        # rows (values)
                        print line, # value
                    
                handle.close()
                print('\n' + table + '.txt read')
                print('')
            else:
                # the file does NOT exist, continue
                continue
            
        print('populate_feeds')
        
    def _register_dataset(self):
        # register the dataset in the management database (dataset table)
        print('register_dataset')
        # clean up mess (temp files, database tables verification records, etc..)
        self._cleanup()
        
    def _cleanup(self):
        #
        print('cleanup')
        
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
        print('updating ' + feed_name)
        
        # exception handling for errors
        result = None
        try:
            print('hi')
            
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
        print('removing ' + feed_name)
        
        # exception handling for errors
        result = None
        try:
            print('hi')
            
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
    usage = "usage: %prog [options] *arguments"
    parser = OptionParser(usage=usage, version="%prog " + str(config.VERSION))
    
    # add the create option
    # usage: "python gtfs.py -c canada_hamilton_hsr http://file.zip Canada Hamilton HSR America/Toronto"
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
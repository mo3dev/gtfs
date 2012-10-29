# coding: utf-8
#!/usr/bin/env python2.7
# gtfs.py

"""GTFS Utility & CRUD class pertaining to a google transit feed.
It can be used to create, update, remove to/from a datastore (via sqlalchemy).
"""

import config
import urllib2
import time
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
            # create the unique dataset_id from the feed_name and current system's timestamp
            timestamp = int(time.time())
            self.dataset_id = feed_name + "_" + str(timestamp)
            
            # validate url
            self._validate_feed_url(feed_url)
            
            # register the new feed with the management database
            self.conn_mngmt.execute(self.feed_table.insert(), [
                {"feed_name": feed_name, "feed_url": feed_url, "feed_country": feed_country, "feed_city": feed_city, "feed_agency": feed_agency, "feed_timezone": feed_timezone}
            ])
            
        except GTFSError, err:
            result = err # GTFS custom exceptions only contain a string
        except Exception, err:
            result = str(err)
        else:
            result = True;
        return result
        
    def _validate_feed_url(self, feed_url):
        # make sure the feed url is valid
        try:
            handle = urllib2.urlopen(urllib2.Request(feed_url))
        except urllib2.URLError, e:
            raise GTFSError("URLError: " + e.reason)
        except urllib2.HTTPError, e:
            raise GTFSError("HTTPError: " + e.code + " while trying to reach " + feed_url)
        
    def update(self, feed_name):
        """Updates a current specific feed in the datastore.
        Usage: object.update( feed_name="canada_hamilton_hsr" )
        """
        print("updating " + feed_name)
        
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
        print("removing " + feed_name)
        
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
    print("gtfs module is loaded.")
# coding: utf-8
#!/usr/bin/env python2.7
# gtfs.py

"""GTFS Utility & CRUD functions pertaining to a google transit feed.
It can be used to create, update, remove to/from a sqlite3 datastore.
"""

from optparse import OptionParser

# code version
_version = 0.1


def create(feed_name, feed_url, feed_country, feed_city, feed_agency, feed_timezone):
    """Creates a new feed into the datastore.
    Usage: gtfs.create( feed_name="canada_hamilton_hsr", feed_url="http://feed.zip",
                        feed_country="Canada", feed_city="Hamilton", feed_agency="HSR",
                        feed_timezone="America/Toronto" )
    """
    print("creating " + feed_name)

def update(feed_name):
    """Updates a current specific feed in the datastore.
    Usage: gtfs.update( feed_name="canada_hamilton_hsr" )
    """
    print("updating " + feed_name)

def remove(feed_name):
    """Removes a current specific feed from the datastore.
    Usage: gtfs.remove( feed_name="canada_hamilton_hsr" )
    """
    print("removing " + feed_name)


def main():
    """Entrypoint of the gtfs script (through the command-line)"""
    
    # Use python's OptionParser class to handle command-line arguments
    usage = "usage: %prog [options] *arguments"
    parser = OptionParser(usage=usage, version="%prog " + str(_version))
    
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
        # check the number of arguments
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
        create(feed_name, feed_url, feed_country, feed_city, feed_agency, feed_timezone)
    
    elif options.feed_name_to_update is not None:
        # execute the update function
        feed_name = options.feed_name_to_update
        # update the feed
        update(feed_name)
    
    elif options.feed_name_to_remove is not None:
        # execute the remove function
        feed_name = options.feed_name_to_remove
        # remove the feed
        remove(feed_name)
    
    else:
        # the user didn't supply the correct options in the script
        parser.error('Invalid command-line option. Use gtfs.py --help for a list of valid options')
    
    # print options and args
    #print("options: " + str(options))
    #print("args: " + str(args))
    

if __name__ == '__main__':
    # run main()
    main()
else:
    # module added
    print("gtfs module is loaded.")
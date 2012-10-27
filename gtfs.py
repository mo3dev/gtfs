# coding: utf-8
#!/usr/bin/python
# gtfs.py

"""GTFS Utility & CRUD functions pertaining to a google transit feed.
It can be used to create, update, remove to/from a sqlite3 datastore.
"""

from optparse import OptionParser

# release version
version = 0.1

def create(**info):
    """Creates a new feed into the datastore."""
    print("creating")

def update(**info):
    """Updates a current specific feed in the datastore."""
    print("updating")

def remove(**info):
    """Removes a current specific feed from the datastore."""
    print("removing")

def main():
    """Entrypoint of the gtfs script."""
    
    # Use python's OptionParser class to handle command line arguments
    usage = "usage: %prog [options] *arguments"
    parser = OptionParser(usage=usage, version="%prog " + str(version))
    
    # add the create option
    # usage: "python gtfs.py -c canada_hamilton_hsr http://file.zip Canada Hamilton HSR America/Toronto"
    parser.add_option("-c", dest="feed_name", action="store",
                      help="create a new feed into the datastore")
    # add the update option
    # usage: "python gtfs.py -u canada_hamilton_hsr"
    parser.add_option("-u", dest="feed_name", action="store",
                      help="update a current feed in the datastore")
    # add the remove/delete option
    # usage: "python gtfs.py -r canada_hamilton_hsr"
    parser.add_option("-r", dest="feed_name", action="store",
                      help="remove a current feed from the datastore")
    
    # assign the options (dest) dictionary into options, and the arguments list into args
    (options, args) = parser.parse_args()
    
    # print options and args
    print("options: " + str(options))
    print("args: " + str(args))

if __name__ == '__main__':
    # run main()
    main()
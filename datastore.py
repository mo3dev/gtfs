# coding: utf-8
#!/usr/bin/env python2.7
# datastore.py


import config
from sqlalchemy import *


class Management:
    """This class will handle the connection to the management database using the sqlalchemy module.
    """
    
    def __init__(self):
        """Sets up management db connection."""
        self.engine = create_engine(config.SQLALCHEMY_ENGINE + config.MANAGEMENT_DB, echo=config.SQLALCHEMY_LOGGING)
        self.metadata = MetaData()
        
        # create tables schema
        self._feed()
        self._dataset()
        # create all tables in the identified schema
        self.metadata.create_all(self.engine)
    
    def get_connection(self):
        """Generates and returns a connection object from the current engine property.
        This is not intended as a singleton. You are responsible for closing the connections
        you get from this method after you finish using them."""
        connection = self.engine.connect()
        return connection
    
    def get_feed_table(self):
        return self.feed_table
    
    def get_dataset_table(self):
        return self.dataset_table
    
    def _feed(self):
        """Create the schema for the feed table"""
        self.feed_table = Table('feed', self.metadata,
            Column('feed_id', Integer, primary_key=True, autoincrement=True),
            Column('feed_name', String),
            Column('feed_url', String),
            Column('feed_country', String),
            Column('feed_city', String),
            Column('feed_agency', String),
            Column('feed_timezone', String)
        )
    
    def _dataset(self):
        """Create the schema for the dataset table"""
        self.dataset_table = Table('dataset', self.metadata,
            Column('dataset_id', String, primary_key=True),
            Column('feed_name', String, ForeignKey("feed.feed_name")),
            Column('date_added', DateTime()),
            Column('last_modified', String),
            Column('dataset_created', Integer, default=0)
        )


class Feeds:
    """This class will handle the connection to the feeds database using the sqlalchemy module.
    """
    
    def __init__(self):
        """Sets up feeds db connection."""
        self.engine = create_engine(config.SQLALCHEMY_ENGINE + config.FEEDS_DB, echo=config.SQLALCHEMY_LOGGING)
        self.metadata = MetaData()
        
        # create tables schema
        self._agency()
        self._stops()
        self._routes()
        self._trips()
        self._stop_times()
        self._calendar()
        self._calendar_dates()
        self._fare_attributes()
        self._fare_rules()
        self._shapes()
        self._frequencies()
        self._transfers()
        self._feed_info()
        
        # create all tables in the identified schema
        self.metadata.create_all(self.engine)
    
    def get_connection(self):
        """Generates and returns a connection object from the current engine property.
        This is not intended as a singleton. You are responsible for closing the connections
        you get from this method after you finish using them."""
        connection = self.engine.connect()
        return connection
    
    def get_agency_table(self):
        return self.agency_table
        
    def get_stops_table(self):
        return self.stops_table
        
    def get_routes_table(self):
        return self.routes_table
        
    def get_trips_table(self):
        return self.trips_table
        
    def get_stop_times_table(self):
        return self.stop_times_table
        
    def get_calendar_table(self):
        return self.calendar_table
        
    def get_calendar_dates_table(self):
        return self.calendar_dates_table
        
    def get_fare_attributes_table(self):
        return self.fare_attributes_table
        
    def get_fare_rules_table(self):
        return self.fare_rules_table
        
    def get_shapes_table(self):
        return self.shapes_table
        
    def get_frequencies_table(self):
        return self.frequencies_table
        
    def get_transfers_table(self):
        return self.transfers_table
        
    def get_feed_info_table(self):
        return self.feed_info_table
    
    def _agency(self):
        """Create the schema for the agency table"""
        self.agency_table = Table('agency', self.metadata,
            Column('dataset_id', String, primary_key=True),
            Column('agency_id', String, primary_key=True),
            Column('agency_name', String),
            Column('agency_url', String),
            Column('agency_timezone', String),
            Column('agency_lang', String),
            Column('agency_phone', String),
            Column('agency_fare_url', String)
        )
    
    def _stops(self):
        """Create the schema for the stops table"""
        self.stops_table = Table('stops', self.metadata,
            Column('dataset_id', String, primary_key=True),
            Column('stop_id', String, primary_key=True),
            Column('stop_code', String),
            Column('stop_name', String),
            Column('stop_desc', String),
            Column('stop_lat', String),
            Column('stop_lon', String),
            Column('zone_id', String),
            Column('stop_url', String),
            Column('location_type', String),
            Column('parent_station', String),
            Column('stop_timezone', String)
        )
    
    def _routes(self):
        """Create the schema for the routes table"""
        self.routes_table = Table('routes', self.metadata,
            Column('dataset_id', String, primary_key=True),
            Column('route_id', String, primary_key=True),
            Column('agency_id', String),
            Column('route_short_name', String),
            Column('route_long_name', String),
            Column('route_desc', String),
            Column('route_type', String),
            Column('route_url', String),
            Column('route_color', String),
            Column('route_text_color', String)
        )
    
    def _trips(self):
        """Create the schema for the trips table"""
        self.trips_table = Table('trips', self.metadata,
            Column('dataset_id', String, primary_key=True),
            Column('route_id', String),
            Column('service_id', String),
            Column('trip_id', String, primary_key=True),
            Column('trip_headsign', String),
            Column('trip_short_name', String),
            Column('direction_id', String),
            Column('block_id', String),
            Column('shape_id', String)
        )
    
    def _stop_times(self):
        """Create the schema for the stop_times table"""
        self.stop_times_table = Table('stop_times', self.metadata,
            Column('dataset_id', String),
            Column('trip_id', String),
            Column('arrival_time', String),
            Column('departure_time', String),
            Column('stop_id', String),
            Column('stop_sequence', String),
            Column('stop_headsign', String),
            Column('pickup_type', String),
            Column('drop_off_type', String),
            Column('shape_dist_traveled', String)
        )
    
    def _calendar(self):
        """Create the schema for the calendar table"""
        self.calendar_table = Table('calendar', self.metadata,
            Column('dataset_id', String, primary_key=True),
            Column('service_id', String, primary_key=True),
            Column('monday', String),
            Column('tuesday', String),
            Column('wednesday', String),
            Column('thursday', String),
            Column('friday', String),
            Column('saturday', String),
            Column('sunday', String),
            Column('start_date', String),
            Column('end_date', String)
        )
    
    def _calendar_dates(self):
        """Create the schema for the calendar_dates table"""
        self.calendar_dates_table = Table('calendar_dates', self.metadata,
            Column('dataset_id', String),
            Column('service_id', String),
            Column('date', String),
            Column('exception_type', String)
        )
    
    def _fare_attributes(self):
        """Create the schema for the fare_attributes table"""
        self.fare_attributes_table = Table('fare_attributes', self.metadata,
            Column('dataset_id', String, primary_key=True),
            Column('fare_id', String, primary_key=True),
            Column('price', String),
            Column('currency_type', String),
            Column('payment_method', String),
            Column('transfers', String),
            Column('transfer_duration', String)
        )
    
    def _fare_rules(self):
        """Create the schema for the fare_rules table"""
        self.fare_rules_table = Table('fare_rules', self.metadata,
            Column('dataset_id', String),
            Column('fare_id', String),
            Column('route_id', String),
            Column('origin_id', String),
            Column('destination_id', String),
            Column('contains_id', String)
        )
    
    def _shapes(self):
        """Create the schema for the shapes table"""
        self.shapes_table = Table('shapes', self.metadata,
            Column('dataset_id', String),
            Column('shape_id', String),
            Column('shape_pt_lat', String),
            Column('shape_pt_lon', String),
            Column('shape_pt_sequence', String),
            Column('shape_dist_traveled', String)
        )
    
    def _frequencies(self):
        """Create the schema for the frequencies table"""
        self.frequencies_table = Table('frequencies', self.metadata,
            Column('dataset_id', String),
            Column('trip_id', String),
            Column('start_time', String),
            Column('end_time', String),
            Column('headway_secs', String),
            Column('exact_times', String)
        )
    
    def _transfers(self):
        """Create the schema for the transfers table"""
        self.transfers_table = Table('transfers', self.metadata,
            Column('dataset_id', String),
            Column('from_stop_id', String),
            Column('to_stop_id', String),
            Column('transfer_type', String),
            Column('min_transfer_time', String)
        )
    
    def _feed_info(self):
        """Create the schema for the feed_info table"""
        self.feed_info_table = Table('feed_info', self.metadata,
            Column('dataset_id', String),
            Column('feed_publisher_name', String),
            Column('feed_publisher_url', String),
            Column('feed_lang', String),
            Column('feed_start_date', String),
            Column('feed_end_date', String),
            Column('feed_version', String)
        )

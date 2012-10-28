# coding: utf-8
#!/usr/bin/env python2.7
# datastore.py

"""This class will act as an interface to the sqlalchemy module. An instance of this class
will act as a mediator between the datastore (via sqlalchemy) and the GTFS script(s)
"""

from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

class Datastore:
    def __init__(self, engine):
        """Sets up db engine."""
        self.engine = create_engine(engine, echo=True)
        self.metadata = MetaData()
    
    def create_tables(self):
        """Creates the schema (metadata) for all GTFS tables"""
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
        
        # create all tables in the identified schema above
        self.metadata.create_all(self.engine)
    
    def _agency(self):
        """Create the schema for the agency table"""
        agency = Table('agency', self.metadata,
            Column('dataset_id', String),
            Column('agency_id', String),
            Column('agency_name', String),
            Column('agency_url', String),
            Column('agency_timezone', String),
            Column('agency_lang', String),
            Column('agency_phone', String),
            Column('agency_fare_url', String),
        )
    
    def _stops(self):
        """Create the schema for the stops table"""
        #
    
    def _routes(self):
        """Create the schema for the routes table"""
        #
    
    def _trips(self):
        """Create the schema for the trips table"""
        #
    
    def _stop_times(self):
        """Create the schema for the stop_times table"""
        #
    
    def _calendar(self):
        """Create the schema for the calendar table"""
        #
    
    def _calendar_dates(self):
        """Create the schema for the calendar_dates table"""
        #
    
    def _fare_attributes(self):
        """Create the schema for the fare_attributes table"""
        #
    
    def _fare_rules(self):
        """Create the schema for the fare_rules table"""
        #
    
    def _shapes(self):
        """Create the schema for the shapes table"""
        #
    
    def _frequencies(self):
        """Create the schema for the frequencies table"""
        #
    
    def _transfers(self):
        """Create the schema for the transfers table"""
        #
    
    def _feed_info(self):
        """Create the schema for the feed_info table"""
        #
    
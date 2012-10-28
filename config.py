# coding: utf-8
#!/usr/bin/env python2.7
# config.py

"""This config file defines application-wide constants."""

"""SQLALCHEMY_ENGINE contains the connection string expected by sqlalchemy's
create_engine() function, without the database name.

Possible values:
"mysql://user:pass@localhost/" for mysql
"sqlite:///" for sqlite
...

"""
SQLALCHEMY_ENGINE = "sqlite:///"
SQLALCHEMY_LOGGING = True

# database names
MANAGEMENT_DB = "management"
FEEDS_DB = "feeds"

# code version
VERSION = 0.1
"""
Module contains all the data models for the database. Classes here actually
represent tables where columns are mapped as attributed. Any class inheriting
from Base is a real table in the database. This is called Object Relational
Mapping in the sqlalchemy or ORM.
"""

from geoalchemy2 import Geometry
from sqlalchemy import Column, Date, DateTime, Float, Integer, String, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class SnowData(object):
    """
    Base class for which all data will have these attributes
    """
    site_name = Column(String(250))
    date = Column(Date)
    time_created = Column(DateTime(timezone=True), server_default=func.now())
    time_updated = Column(DateTime(timezone=True), onupdate=func.now())
    id = Column(Integer, primary_key=True)
    doi = Column(String(50))
    date_accessed = Column(Date)


class SingleLocationData(SnowData):
    """
    Base class for points and profiles
    """
    latitude = Column(Float)
    longitude = Column(Float)
    northing = Column(Float)
    easting = Column(Float)
    elevation = Column(Float)
    utm_zone = Column(Integer)
    geom = Column(Geometry("POINT"))
    time = Column(Time(timezone=True))
    site_id = Column(String(50))


class Measurement(object):
    """
    Base Class providing attributes required for a measurement of any kind
    """
    type = Column(String(50))
    units = Column(String(50))

from sqlalchemy import Column, Float, Integer, String, ForeignKey
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from typing import List
from sqlalchemy.orm import relationship

from .base import Base, Measurement, SingleLocationData
from .instrument import Instrument


class PointData(SingleLocationData, Measurement, Base):
    """
    Class representing the points table. This table holds all point data.
    Here a single data entry is a single coordinate pair with a single value
    e.g. snow depths
    """
    __tablename__ = 'points'
    __table_args__ = {"schema": "public"}

    version_number = Column(Integer)
    equipment = Column(String(50))
    value = Column(Float)

    # bring these in instead of Measurement
    type = Column(String(50))
    units = Column(String(50))

    # Link the instrument id with a foreign key
    instrument_id = Column(Integer, ForeignKey('public.instruments.id'))
    # Link the Instrument class
    instrument = relationship('Instrument')

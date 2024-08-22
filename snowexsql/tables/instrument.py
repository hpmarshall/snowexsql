from sqlalchemy import Column, Integer, String
from .base import Base


class Instrument(Base):
    __tablename__ = 'instruments'
    __table_args__ = {"schema": "public"}
     # this seems important
    __mapper_args__ = {
        'polymorphic_identity': 'Instruments',
    }
    # auto created id
    id = Column(Integer, primary_key=True)
    # Name of the instrument
    name = Column(String(50))
    model = Column(String(255))
    specifications = Column(String(255))

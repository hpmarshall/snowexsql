from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy import Column, String

from .base import Base


class Observer(Base):
    __tablename__ = 'observers'
    __table_args__ = {"schema": "public"}
    # id is mapped column for many-to-many
    id: Mapped[int] = mapped_column(primary_key=True)
    # Name of the observer
    first_name = Column(String(255))
    last_name = Column(String(255))

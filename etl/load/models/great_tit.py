from sqlalchemy import Column, Integer, Date
from config import SQLALCHEMY_BASE
from geoalchemy2.types import Geometry


class GreatTit(SQLALCHEMY_BASE):
    __tablename__ = 'great_tit'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    count = Column(Integer)
    geometry = Column(Geometry('POINT'))
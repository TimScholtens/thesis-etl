from sqlalchemy import Column, Integer, String, Date
from config import SQLALCHEMY_BASE
from geoalchemy2.types import Geometry


class OakProcessionaryMoth(SQLALCHEMY_BASE):
    __tablename__ = 'oak_processionary_moths'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    stage = Column(String, nullable=True)
    geometry = Column(Geometry('POINT'))

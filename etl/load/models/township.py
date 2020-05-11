from sqlalchemy import Column, Integer, String
from config import SQLALCHEMY_BASE
from geoalchemy2.types import Geometry


class Township(SQLALCHEMY_BASE):
    __tablename__ = 'township_locations'
    code = Column(Integer, primary_key=True)
    name = Column(String)
    geometry = Column(Geometry)


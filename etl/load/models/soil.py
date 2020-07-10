from sqlalchemy import Column, Integer, String
from config import SQLALCHEMY_BASE
from geoalchemy2.types import Geometry


class Soil(SQLALCHEMY_BASE):
    __tablename__ = 'soil'
    id = Column(Integer, primary_key=True, autoincrement=True)
    soil_type = Column(String)
    geometry = Column(Geometry)
from sqlalchemy import Column, Integer, String
from config import SQLALCHEMY_BASE
from geoalchemy2.types import Geometry


class Township(SQLALCHEMY_BASE):
    __tablename__ = 'townships'
    code = Column(Integer, primary_key=True)
    name = Column(String)
    geometry = Column(Geometry)


class Neighbourhood(SQLALCHEMY_BASE):
    __tablename__ = 'neighbourhoods'
    id = Column(String, primary_key=True)
    township = Column(String)
    name = Column(String)
    geometry = Column(Geometry)

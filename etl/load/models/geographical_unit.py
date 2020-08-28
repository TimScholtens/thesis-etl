from sqlalchemy import Column, Integer, String, Float
from config import SQLALCHEMY_BASE
from geoalchemy2.types import Geometry


class Township(SQLALCHEMY_BASE):
    __tablename__ = 'townships'
    code = Column(Integer, primary_key=True)
    name = Column(String)
    geometry = Column(Geometry)


class Neighbourhood(SQLALCHEMY_BASE):
    __tablename__ = 'neighbourhoods'
    code = Column(String, primary_key=True)
    township = Column(String)
    name = Column(String)
    area = Column(Float)
    geometry = Column(Geometry)


class Province(SQLALCHEMY_BASE):
    __tablename__ = 'provinces'
    code = Column(String, primary_key=True)
    name = Column(String)
    geometry = Column(Geometry)

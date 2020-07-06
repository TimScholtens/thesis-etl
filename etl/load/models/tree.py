from sqlalchemy import Column, Integer, String
from config import SQLALCHEMY_BASE
from geoalchemy2.types import Geometry


class Tree(SQLALCHEMY_BASE):
    __tablename__ = 'tree'
    id = Column(Integer, primary_key=True, autoincrement=True)
    # species_latin = Column(String)
    species_dutch = Column(String)
    geometry = Column(Geometry('POINT'))

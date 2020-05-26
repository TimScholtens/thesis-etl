from sqlalchemy import Column, Integer, String, Float
from config import SQLALCHEMY_BASE


class BioClim_1(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_1'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    temperature_avg_year = Column(Float(precision=2, asdecimal=True))



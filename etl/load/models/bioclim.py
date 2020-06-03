from sqlalchemy import Column, Integer, String, Float, Date
from config import SQLALCHEMY_BASE


class BioClim_1(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_1'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    date = Column(Date)
    temperature_avg_year = Column(Float(precision=2, asdecimal=True))



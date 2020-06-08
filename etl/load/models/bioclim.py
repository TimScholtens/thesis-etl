from sqlalchemy import Column, Integer, String, Float, Date
from config import SQLALCHEMY_BASE


class BioClim_1(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_1'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    date = Column(Date)
    temperature_avg = Column(Float(precision=2, asdecimal=True))


class BioClim_2(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_2'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    date = Column(Date)
    diurmal_range = Column(Float(precision=2, asdecimal=True))


class BioClim_5(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_5'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    date = Column(Date)
    temperature_max = Column(Float(precision=2, asdecimal=True))


class BioClim_6(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_6'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    date = Column(Date)
    temperature_min = Column(Float(precision=2, asdecimal=True))


class BioClim_7(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_7'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    date = Column(Date)
    diurmal_range = Column(Float(precision=2, asdecimal=True))

from sqlalchemy import Column, Integer, String, Float
from config import SQLALCHEMY_BASE


class BioClim_1(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_1'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    temperature_avg = Column(Float(precision=2, asdecimal=True))


class BioClim_2(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_2'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    diurmal_range = Column(Float(precision=2, asdecimal=True))


class BioClim_5(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_5'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    temperature_max = Column(Float(precision=2, asdecimal=True))


class BioClim_6(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_6'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    temperature_min = Column(Float(precision=2, asdecimal=True))


class BioClim_7(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_7'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    diurmal_range = Column(Float(precision=2, asdecimal=True))


class BioClim_10(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_10'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    temperature_avg = Column(Float(precision=2, asdecimal=True))


class BioClim_11(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_11'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    temperature_avg = Column(Float(precision=2, asdecimal=True))


class BioClim_12(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_12'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    rain_sum = Column(Float(precision=2, asdecimal=True))


class BioClim_13(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_13'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    rain_sum = Column(Float(precision=2, asdecimal=True))


class BioClim_14(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_14'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    rain_sum = Column(Float(precision=2, asdecimal=True))


class BioClim_16(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_16'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    rain_sum = Column(Float(precision=2, asdecimal=True))


class BioClim_17(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_17'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    rain_sum = Column(Float(precision=2, asdecimal=True))


class BioClim_18(SQLALCHEMY_BASE):
    __tablename__ = 'bioclim_18'
    id = Column(Integer, primary_key=True, autoincrement=True)
    township = Column(String)
    year = Column(Integer)
    rain_sum = Column(Float(precision=2, asdecimal=True))

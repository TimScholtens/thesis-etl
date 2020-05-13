from sqlalchemy import Column, Integer, String, Date, Float
from config import SQLALCHEMY_BASE
from geoalchemy2.types import Geometry


class WeatherStationLocation(SQLALCHEMY_BASE):
    __tablename__ = 'weather_station_locations'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geometry = Column(Geometry('POINT'))


class WeatherStationData(SQLALCHEMY_BASE):
    __tablename__ = 'weather_station_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(Integer)
    date = Column(Date)
    temperature_avg = Column(Float(precision=2, asdecimal=True))
    temperature_min = Column(Float(precision=2, asdecimal=True))
    temperature_max = Column(Float(precision=2, asdecimal=True))
    sunshine_duration = Column(Float(precision=2, asdecimal=True))
    sunshine_radiation = Column(Float(precision=2, asdecimal=True))
    rain_duration = Column(Float(precision=2, asdecimal=True))
    rain_sum = Column(Float(precision=2, asdecimal=True))
    humidity_avg = Column(Float(precision=2, asdecimal=True))
    humidity_max = Column(Float(precision=2, asdecimal=True))
    humidity_min = Column(Float(precision=2, asdecimal=True))

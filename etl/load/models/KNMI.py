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
    temperature_avg = Column(Float)
    temperature_min = Column(Float)
    temperature_max = Column(Float)
    sunshine_duration = Column(Float)
    sunshine_radiation = Column(Float)
    rain_duration = Column(Float)
    rain_sum = Column(Float)
    humidity_avg = Column(Float)
    humidity_max = Column(Float)
    humidity_min = Column(Float)
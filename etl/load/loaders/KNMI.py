import csv
from datetime import datetime
from shapely.geometry import Point
from etl.load.loaders.base import Base
from etl.load.loader import final_transformation_file
from sqlalchemy.orm import sessionmaker
from decimal import Decimal
from config import SQLALCHEMY_ENGINE
from etl.load.models.KNMI import WeatherStationData as WeatherStationDataObject
from etl.load.models.KNMI import WeatherStationLocation as WeatherStationLocationObject


class KNMIWeatherStationData(Base):

    def load(self, transform_directory):

        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_NONE)  # quote non to skip whitespace

            weather_station_data = [dict(
                station_id=int(row['station_id']),
                date=datetime.strptime(row['date'], "%Y-%m-%d"),
                temperature_avg=Decimal(row['temperature_avg']),
                temperature_min=Decimal(row['temperature_min']),
                temperature_max=Decimal(row['temperature_max']),
                sunshine_duration=Decimal(row['sunshine_duration']),
                sunshine_radiation=Decimal(row['sunshine_radiation']),
                rain_duration=Decimal(row['rain_duration']),
                rain_sum=Decimal(row['rain_sum']),
                humidity_avg=Decimal(row['humidity_avg']),
                humidity_max=Decimal(row['humidity_max']),
                humidity_min=Decimal(row['humidity_min'])
            ) for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()

        session.bulk_insert_mappings(mapper=WeatherStationDataObject,
                                     mappings=weather_station_data,
                                     render_nulls=True,
                                     return_defaults=False)
        session.commit()
        session.close()


class KNMIWeatherStationLocation(Base):

    def load(self, transform_directory):

        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',')  # quote non to skip whitespace

            weather_station_locations = [dict(
                id=row['STN'],
                name=row['NAME'],
                geometry=Point(float(row['LON(east)']), float(row['LAT(north)'])).wkt
            ) for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()
        session.bulk_insert_mappings(mapper=WeatherStationLocationObject,
                                     mappings=weather_station_locations,
                                     render_nulls=True,
                                     return_defaults=False)
        session.commit()
        session.close()

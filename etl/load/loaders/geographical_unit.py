import json
import csv
from shapely.geometry import shape
from etl.load.loaders.base import Base
from etl.load.loader import final_transformation_file
from sqlalchemy.orm import sessionmaker
from config import SQLALCHEMY_ENGINE
from etl.load.models.geographical_unit import (
    Township as TownshipObject,
    Neighbourhood as NeighbourhoodObject,
    Province as ProvinceObject
)


class Township(Base):

    def load(self, transform_directory):
        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            json_file = json.load(f)

        townships = [TownshipObject(name=line['properties']['name'],
                                    code=line['properties']['code'],
                                    geometry=shape(line['geometry']).wkt) for line in json_file['features']]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()
        session.add_all(townships)
        session.commit()
        session.close()


class Neighbourhood(Base):

    def load(self, transform_directory):
        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_ALL)

            neighbourhoods = [dict(
                code=row['id'],
                name=row['name'],
                township=row['township'],
                geometry=row['geometry']

            ) for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()
        session.bulk_insert_mappings(mapper=NeighbourhoodObject,
                                     mappings=neighbourhoods,
                                     render_nulls=True,
                                     return_defaults=False)

        session.commit()
        session.close()


class Province(Base):

    def load(self, transform_directory):
        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_ALL)

            provinces = [dict(
                code=row['id'],
                name=row['name'],
                geometry=row['geometry']

            ) for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()
        session.bulk_insert_mappings(mapper=ProvinceObject,
                                     mappings=provinces,
                                     render_nulls=True,
                                     return_defaults=False)

        session.commit()
        session.close()

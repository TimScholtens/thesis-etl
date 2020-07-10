import csv
from etl.load.loaders.base import Base
from etl.load.loader import final_transformation_file
from sqlalchemy.orm import sessionmaker
from etl.load.models.soil import Soil as SoilObject
from config import SQLALCHEMY_ENGINE


class WURAlterra(Base):

    def load(self, transform_directory):
        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_ALL)

            soil = [dict(
                date=row['date'],
                soil_type=row['soil_type'],
                geometry=row['geometry']

            ) for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()
        session.bulk_insert_mappings(mapper=SoilObject,
                                     mappings=soil,
                                     render_nulls=True,
                                     return_defaults=False)

        session.commit()
        session.close()
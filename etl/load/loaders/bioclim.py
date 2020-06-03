import json
from etl.load.loaders.base import Base
from etl.load.loader import final_transformation_file
from sqlalchemy.orm import sessionmaker
import csv
from decimal import Decimal
from datetime import datetime


class BioClim_1(Base):

    def load(self, transform_directory):
        # Workaround - Import here to prevent cyclic import
        from etl.load.models.bioclim import BioClim_1 as BioClim_1_Object
        import config

        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_ALL)

            townships_temperature_avg_year = [dict(
                township=row['township'],
                date=datetime.strptime(row['date'], "%Y-%m-%d"),
                temperature_avg_year=Decimal(row['interpolated_values'])
            ) for row in csv_reader]

        session = sessionmaker(bind=config.SQLALCHEMY_ENGINE)()

        session.bulk_insert_mappings(mapper=BioClim_1_Object,
                                     mappings=townships_temperature_avg_year,
                                     render_nulls=True,
                                     return_defaults=False)
        session.commit()
        session.close()

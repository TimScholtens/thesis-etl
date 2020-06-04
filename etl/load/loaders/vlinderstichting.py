import csv
from etl.load.loaders.base import Base
from etl.load.loader import final_transformation_file
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from etl.load.models.vlinderstichting import OakProcessionaryMoth as OakProcessionaryMothObject
from config import SQLALCHEMY_ENGINE


class Vlinderstichting(Base):

    def load(self, transform_directory):
        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_NONE)  # quote non to skip whitespace

            oak_processionary_moths = [dict(
                date=datetime.strptime(row['date'], '%Y-%m-%d').date(),
                stage=row['stage'],
                geometry=row['geometry']

            ) for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()
        session.bulk_insert_mappings(mapper=OakProcessionaryMothObject,
                                     mappings=oak_processionary_moths,
                                     render_nulls=True,
                                     return_defaults=False)

        session.commit()
        session.close()

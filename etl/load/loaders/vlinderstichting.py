from etl.load.loaders.base import Base
from etl.load.loader import final_transformation_file
from sqlalchemy.orm import sessionmaker
import csv
from datetime import datetime


class Vlinderstichting(Base):

    def load(self, transform_directory):
        # Workaround - Import here to prevent cyclic import
        from etl.load.models.vlinderstichting import OakProcessionaryMoth as OakProcessionaryMothObject
        import config

        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_NONE)  # quote non to skip whitespace

            oak_processionary_moths = [dict(
                date=datetime.strptime(row['date'], '%Y-%m-%d').date(),
                stage=row['stage'],
                geometry=row['geometry']

            ) for row in csv_reader]

        session = sessionmaker(bind=config.SQLALCHEMY_ENGINE)()
        session.bulk_insert_mappings(mapper=OakProcessionaryMothObject,
                                     mappings=oak_processionary_moths,
                                     render_nulls=True,
                                     return_defaults=False)

        session.commit()
        session.close()

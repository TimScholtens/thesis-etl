import csv
from etl.load.loaders.base import Base
from etl.load.loader import final_transformation_file
from sqlalchemy.orm import sessionmaker
from etl.load.models.opm import OakProcessionaryMoth as OakProcessionaryMothObject
from config import SQLALCHEMY_ENGINE


class Vlinderstichting(Base):

    def load(self, transform_directory):
        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_NONE)  # quote non to skip whitespace

            oak_processionary_moths = [dict(
                date=row['date'],
                stage=row['stage'],
                geometry=row['geometry'],
                origin='vlinderstichting',
                granularity='moth'

            ) for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()
        session.bulk_insert_mappings(mapper=OakProcessionaryMothObject,
                                     mappings=oak_processionary_moths,
                                     render_nulls=True,
                                     return_defaults=False)

        session.commit()
        session.close()


class Amsterdam(Base):

    def load(self, transform_directory):
        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_NONE)  # quote non to skip whitespace

            oak_processionary_moths = [dict(
                date=row['date'],
                stage=None,
                geometry=row['geometry'],
                origin='amsterdam',
                granularity='nest'

            ) for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()
        session.bulk_insert_mappings(mapper=OakProcessionaryMothObject,
                                     mappings=oak_processionary_moths,
                                     render_nulls=True,
                                     return_defaults=False)

        session.commit()
        session.close()


class Gelderland(Base):

    def load(self, transform_directory):
        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_NONE)  # quote non to skip whitespace

            oak_processionary_moths = [dict(
                date=row['date'],
                stage=None,
                geometry=row['geometry'],
                origin='gelderland',
                granularity='nest'

            ) for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()
        session.bulk_insert_mappings(mapper=OakProcessionaryMothObject,
                                     mappings=oak_processionary_moths,
                                     render_nulls=True,
                                     return_defaults=False)

        session.commit()
        session.close()

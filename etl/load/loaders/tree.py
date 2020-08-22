import csv
from etl.load.loaders.base import Base
from etl.load.loader import final_transformation_file
from sqlalchemy.orm import sessionmaker
from etl.load.models.tree import Tree as TreeObject
from config import SQLALCHEMY_ENGINE


class Amsterdam(Base):

    def load(self, transform_directory):
        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_NONE)  # quote non to skip whitespace

            trees = [dict(
                # species_latin=row['species_latin'],
                species_dutch=row['species_dutch'],
                geometry=row['geometry'],
                origin='Amsterdam'

            ) for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()
        session.bulk_insert_mappings(mapper=TreeObject,
                                     mappings=trees,
                                     render_nulls=True,
                                     return_defaults=False)

        session.commit()
        session.close()


class Gelderland(Base):

    def load(self, transform_directory):
        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_ALL)  # quote non to skip whitespace

            trees = [dict(
                # species_latin=row['species_latin'],
                species_dutch=row['species_dutch'],
                geometry=row['geometry'],
                origin='Gelderland'

            ) for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()
        session.bulk_insert_mappings(mapper=TreeObject,
                                     mappings=trees,
                                     render_nulls=True,
                                     return_defaults=False)

        session.commit()
        session.close()

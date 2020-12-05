import csv
from etl.load.loaders.base import Base
from etl.load.loader import final_transformation_file
from sqlalchemy.orm import sessionmaker
from etl.load.models.great_tit import GreatTit as GreatTitObject
from config import SQLALCHEMY_ENGINE


class GreatTit(Base):

    def load(self, transform_directory):
        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_NONE)  # quote non to skip whitespace

            great_tits = [dict(
                date=row['date'],
                count=row['count'],
                geometry=row['geometry']

            ) for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()
        session.bulk_insert_mappings(mapper=GreatTitObject,
                                     mappings=great_tits,
                                     render_nulls=True,
                                     return_defaults=False)

        session.commit()
        session.close()

import json
from shapely.geometry import shape
from etl.load.loaders.base import Base
from etl.load.loader import final_transformation_file
from sqlalchemy.orm import sessionmaker
from config import SQLALCHEMY_ENGINE
from etl.load.models.geographical_unit import Township as TownshipObject


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

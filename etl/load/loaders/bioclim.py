import csv
import etl.load.models.bioclim as bioclim_models
from etl.load.loaders.base import Base
from etl.load.loader import final_transformation_file
from sqlalchemy.orm import sessionmaker
from decimal import Decimal
from config import SQLALCHEMY_ENGINE
from enum import Enum
from datetime import datetime


class BioClim(Base):

    def __init__(self, model, interpolated_value_name):
        self._model = model
        self._interpolated_value_name = interpolated_value_name

    @property
    def model(self):
        return self._model

    @property
    def interpolated_value_name(self):
        return self._interpolated_value_name

    def load(self, transform_directory):
        file_path = transform_directory / final_transformation_file(transform_directory=transform_directory)

        with open(file_path) as f:
            csv_reader = csv.DictReader(f, delimiter=',', quoting=csv.QUOTE_ALL)

            townships_interpolated = [{
                "code": row['id'],
                "name": row['name'],
                "township": row['township'],
                "year": datetime.strptime(row['year'], "%Y-%m-%d %H:%M:%S").year,
                self.interpolated_value_name: Decimal(row['interpolated_values'])
            } for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()

        session.bulk_insert_mappings(mapper=self.model,
                                     mappings=townships_interpolated,
                                     render_nulls=True,
                                     return_defaults=False)
        session.commit()
        session.close()


class BioClimEnums(Enum):
    bioclim_1 = 'bioclim_1'
    bioclim_2 = 'bioclim_2'
    bioclim_3 = 'bioclim_3'
    bioclim_4 = 'bioclim_4'
    bioclim_5 = 'bioclim_5'
    bioclim_6 = 'bioclim_6'
    bioclim_7 = 'bioclim_7'
    bioclim_8 = 'bioclim_8'
    bioclim_9 = 'bioclim_9'
    bioclim_10 = 'bioclim_10'
    bioclim_11 = 'bioclim_11'
    bioclim_12 = 'bioclim_12'
    bioclim_13 = 'bioclim_13'
    bioclim_14 = 'bioclim_14'
    bioclim_15 = 'bioclim_15'
    bioclim_16 = 'bioclim_16'
    bioclim_17 = 'bioclim_17'
    bioclim_18 = 'bioclim_18'
    bioclim_19 = 'bioclim_19'


class BioClimFactory:

    @staticmethod
    def get_bioclim(bioclim_id):
        if bioclim_id is BioClimEnums.bioclim_1:
            return BioClim(model=bioclim_models.BioClim_1, interpolated_value_name='temperature_avg')
        elif bioclim_id is BioClimEnums.bioclim_2:
            return BioClim(model=bioclim_models.BioClim_2, interpolated_value_name='diurmal_range')
        elif bioclim_id is BioClimEnums.bioclim_3:
            return BioClim(model=bioclim_models.BioClim_3, interpolated_value_name='isothermality')
        elif bioclim_id is BioClimEnums.bioclim_4:
            return BioClim(model=bioclim_models.BioClim_4, interpolated_value_name='temperature_std')
        elif bioclim_id is BioClimEnums.bioclim_5:
            return BioClim(model=bioclim_models.BioClim_5, interpolated_value_name='max_temperature')
        elif bioclim_id is BioClimEnums.bioclim_6:
            return BioClim(model=bioclim_models.BioClim_6, interpolated_value_name='min_temperature')
        elif bioclim_id is BioClimEnums.bioclim_7:
            return BioClim(model=bioclim_models.BioClim_7, interpolated_value_name='diurmal_range')
        else:
            raise NotImplementedError

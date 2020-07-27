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
            return BioClim(model=bioclim_models.BioClim_5, interpolated_value_name='temperature_min')
        elif bioclim_id is BioClimEnums.bioclim_6:
            return BioClim(model=bioclim_models.BioClim_6, interpolated_value_name='temperature_max')
        elif bioclim_id is BioClimEnums.bioclim_7:
            return BioClim(model=bioclim_models.BioClim_7, interpolated_value_name='diurmal_range')
        elif bioclim_id is BioClimEnums.bioclim_8:
            return BioClim(model=bioclim_models.BioClim_8, interpolated_value_name='temperature_avg')
        elif bioclim_id is BioClimEnums.bioclim_9:
            return BioClim(model=bioclim_models.BioClim_9, interpolated_value_name='temperature_avg')
        elif bioclim_id is BioClimEnums.bioclim_10:
            return BioClim(model=bioclim_models.BioClim_10, interpolated_value_name='temperature_avg')
        elif bioclim_id is BioClimEnums.bioclim_11:
            return BioClim(model=bioclim_models.BioClim_11, interpolated_value_name='temperature_avg')
        elif bioclim_id is BioClimEnums.bioclim_12:
            return BioClim(model=bioclim_models.BioClim_12, interpolated_value_name='rain_sum')
        elif bioclim_id is BioClimEnums.bioclim_13:
            return BioClim(model=bioclim_models.BioClim_13, interpolated_value_name='rain_sum')
        elif bioclim_id is BioClimEnums.bioclim_14:
            return BioClim(model=bioclim_models.BioClim_14, interpolated_value_name='rain_sum')
        elif bioclim_id is BioClimEnums.bioclim_15:
            return BioClim(model=bioclim_models.BioClim_15, interpolated_value_name='rain_sum')
        elif bioclim_id is BioClimEnums.bioclim_16:
            return BioClim(model=bioclim_models.BioClim_16, interpolated_value_name='rain_sum')
        elif bioclim_id is BioClimEnums.bioclim_17:
            return BioClim(model=bioclim_models.BioClim_17, interpolated_value_name='rain_sum')
        elif bioclim_id is BioClimEnums.bioclim_18:
            return BioClim(model=bioclim_models.BioClim_18, interpolated_value_name='rain_sum')
        elif bioclim_id is BioClimEnums.bioclim_19:
            return BioClim(model=bioclim_models.BioClim_19, interpolated_value_name='rain_sum')
        else:
            raise NotImplementedError

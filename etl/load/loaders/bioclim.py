import csv
import etl.load.models.bioclim as bioclim_models
from etl.load.loaders.base import Base
from etl.load.loader import final_transformation_file
from sqlalchemy.orm import sessionmaker
from decimal import Decimal
from datetime import datetime
from config import SQLALCHEMY_ENGINE


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
                "township": row['township'],
                "year": row['year'],
                self.interpolated_value_name: Decimal(row['interpolated_values'])
            } for row in csv_reader]

        session = sessionmaker(bind=SQLALCHEMY_ENGINE)()

        session.bulk_insert_mappings(mapper=self.model,
                                     mappings=townships_interpolated,
                                     render_nulls=True,
                                     return_defaults=False)
        session.commit()
        session.close()


class BioClim_1(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_1, interpolated_value_name='temperature_avg')


class BioClim_2(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_2, interpolated_value_name='diurmal_range')

class BioClim_3(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_3, interpolated_value_name='isothermality')

class BioClim_4(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_4, interpolated_value_name='temperature_std')


class BioClim_5(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_5, interpolated_value_name='temperature_max')


class BioClim_6(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_6, interpolated_value_name='temperature_min')


class BioClim_7(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_7, interpolated_value_name='diurmal_range')


class BioClim_8(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_8, interpolated_value_name='temperature_avg')


class BioClim_9(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_9, interpolated_value_name='temperature_avg')


class BioClim_10(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_10, interpolated_value_name='temperature_avg')


class BioClim_11(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_11, interpolated_value_name='temperature_avg')


class BioClim_12(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_12, interpolated_value_name='rain_sum')


class BioClim_13(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_13, interpolated_value_name='rain_sum')


class BioClim_14(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_14, interpolated_value_name='rain_sum')


class BioClim_16(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_16, interpolated_value_name='rain_sum')


class BioClim_17(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_17, interpolated_value_name='rain_sum')


class BioClim_18(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_18, interpolated_value_name='rain_sum')


class BioClim_19(BioClim):

    def __init__(self):
        super().__init__(model=bioclim_models.BioClim_19, interpolated_value_name='rain_sum')

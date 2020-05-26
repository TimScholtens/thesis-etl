import os
from decimal import getcontext
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from etl.transform.transformers.dummy import Dummy as DummyTransformer
from etl.transform.transformers.passthrough import Passthrough as PassthroughTransformer
from etl.transform.transformers.KNMI import KNMIWeatherStationData as KNMIWeatherStationDataTransformer
from etl.transform.transformers.bioclim import BioClim_1 as BioClim_1_Transformer
from etl.load.loaders.township import Township as TownshipLoader
from etl.load.loaders.dummy import Dummy as DummyLoader
from etl.load.loaders.KNMI import KNMIWeatherStationLocation as KNMIWeatherStationLocationLoader
from etl.load.loaders.KNMI import KNMIWeatherStationData as KNMIWeatherStationDataLoader
from etl.load.loaders.bioclim import BioClim_1 as BioClim_1_Loader


class ETLConfigItem:
    ETL_BASE_DIRECTORY = Path.cwd() / 'static' / 'etl'
    EXTRACT_DIRECTORY = ETL_BASE_DIRECTORY / 'extract'
    TRANSFORM_DIRECTORY = ETL_BASE_DIRECTORY / 'transform'

    def __init__(self, name,
                 gs_uris,
                 transformer=DummyTransformer(),
                 loader=DummyLoader()):
        """
        :param name: name of etl config item.
        :param gs_uris: list of file locations within google cloud storage . eg.
        eg. [gs://vaa-opm/knmi/weather_station_data.json, gs://vaa-opm/knmi/weather_station_locations.json']
        :param transformer: transformer class to use for transforming data.
        :param loader: loader class to use for loading data into database.
        """
        self._name = name
        self._source_location = gs_uris
        self._transformer = transformer
        self._loader = loader

    @property
    def name(self):
        return self._name

    @property
    def gs_uris(self):
        return self._source_location

    @property
    def extract_location(self):
        """Directory where extracted data will be saved"""
        return ETLConfigItem.EXTRACT_DIRECTORY / f'{self._name}'

    @property
    def transform_location(self):
        """Directory where transformed data will be saved"""
        return ETLConfigItem.TRANSFORM_DIRECTORY / f'{self._name}'

    @property
    def transformer(self):
        return self._transformer

    @property
    def loader(self):
        return self._loader


# noinspection PyTypeChecker
ETL_CONFIG_ITEMS = [

    ETLConfigItem(name='KNMI_weather_station_data',
                  gs_uris=['gs://vaa-opm/KNMI/station_data.csv'],
                  transformer=KNMIWeatherStationDataTransformer(),
                  loader=KNMIWeatherStationDataLoader()),
    ETLConfigItem(name='KNMI_weather_station_locations',
                  gs_uris=['gs://vaa-opm/KNMI/station_locations.csv'],
                  transformer=PassthroughTransformer(),
                  loader=KNMIWeatherStationLocationLoader()),
    ETLConfigItem(name='Townships',
                  gs_uris=['gs://vaa-opm/Townships/townships.json'],
                  transformer=PassthroughTransformer(),
                  loader=TownshipLoader()),
    ETLConfigItem(name='BIOCLIM_1',
                  gs_uris=['gs://vaa-opm/KNMI/station_data.csv',
                           'gs://vaa-opm/KNMI/station_locations.csv',
                           'gs://vaa-opm/Townships/townships.json'],
                  transformer=BioClim_1_Transformer(),
                  loader=BioClim_1_Loader())
]

# Debug config
DEBUG = 1


# Set decimal precision
getcontext().prec = 2

# Final transformation ID
FINAL_TRANSFORMATION_ID = 'FINAL'

# Set google cloud config
GCP_BUCKET = 'vaa-opm'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(
    Path.cwd() / 'static' / 'secrets' / 'cloud_storage_key.json')  # Authentication

# Set postresql/postgis config
SQLALCHEMY_ENGINE = create_engine('postgres://tim:doyouopm@localhost:5432/opm',
                                  echo=DEBUG,
                                  executemany_mode='values',
                                  executemany_values_page_size=10000,
                                  client_encoding='utf8')
SQLALCHEMY_BASE = declarative_base()

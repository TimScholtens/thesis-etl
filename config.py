import os
from decimal import getcontext
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from etl.transform.transformers.township import Township as TownshipTransformer
from etl.transform.transformers.dummy import Dummy as DummyTransformer
from etl.transform.transformers.KNMI import  KNMI as KNMITransformer
from etl.load.loaders.township import Township as TownshipLoader
from etl.load.loaders.dummy import Dummy as DummyLoader
from etl.load.loaders.KNMI import KNMI as KNMILoader


class ETLConfigItem:

    def __init__(self, name,
                 gs_uris,
                 extract_location,
                 transform_location,
                 transformer=DummyTransformer(),
                 loader=DummyLoader()):
        """
        :param name: name of etl config item.
        :param gs_uris: list of file locations within google cloud storage . eg.
        eg. [gs://vaa-opm/knmi/weather_station_data.json, gs://vaa-opm/knmi/weather_station_locations.json']
        :param extract_location: directory where extracted data will be saved.
        :param transform_location: directory where transformed data will be saved.
        :param transformer: transformer class to use for transforming data.
        :param loader: loader class to use for loading data into database.
        """
        self._name = name
        self._source_location = gs_uris
        self._extract_location = extract_location
        self._transform_location = transform_location
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
        return self._extract_location

    @property
    def transform_location(self):
        return self._transform_location

    @property
    def transformer(self):
        return self._transformer

    @property
    def loader(self):
        return self._loader


DEBUG = 0

ETL_BASE_DIRECTORY = Path.cwd() / 'static' / 'etl'
EXTRACT_DIRECTORY = ETL_BASE_DIRECTORY / 'extract'
TRANSFORM_DIRECTORY = ETL_BASE_DIRECTORY / 'transform'

# noinspection PyTypeChecker
ETL_CONFIG_ITEMS = [ETLConfigItem(name='Nationale databank flora en fauna',
                                  gs_uris=['gs://vaa-opm/NDFF/NDFF-export_03-03-2020_09-33-45.dbf'],
                                  extract_location=EXTRACT_DIRECTORY / 'NDFF',
                                  transform_location=TRANSFORM_DIRECTORY / 'NDFF'),
                    ETLConfigItem(name='Koninkelijk nationaal metreologisch instituut',
                                  gs_uris=['gs://vaa-opm/KNMI/station_data.csv', 'gs://vaa-opm/KNMI/station_locations.csv'],
                                  extract_location=EXTRACT_DIRECTORY / 'KNMI',
                                  transform_location=TRANSFORM_DIRECTORY / 'KNMI',
                                  transformer=KNMITransformer(),
                                  loader=KNMILoader()),
                    ETLConfigItem(name='Townships',
                                  gs_uris=['gs://vaa-opm/Townships/townships.json'],
                                  extract_location=EXTRACT_DIRECTORY / 'Townships',
                                  transformer=TownshipTransformer(),
                                  transform_location=TRANSFORM_DIRECTORY / 'Townships',
                                  loader=TownshipLoader()),
                    ETLConfigItem(name='Boomregister',
                                  gs_uris=['gs://vaa-opm/Boomregister/den_bosch.dbf'],
                                  extract_location=EXTRACT_DIRECTORY / 'Boomregister',
                                  transform_location=TRANSFORM_DIRECTORY / 'Boomregister')
                    ]

# Set decimal precision
getcontext().prec = 2

# Set google cloud config
GCP_BUCKET = 'vaa-opm'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(
    Path.cwd() / 'static' / 'secrets' / 'cloud_storage_key.json'
)  # Authentication

# Set postresql/postgis config
SQLALCHEMY_ENGINE = create_engine('postgresql+psycopg2://tim:doyouopm@localhost:5432/opm',
                                  echo=DEBUG,
                                  executemany_mode='values',
                                  executemany_values_page_size=10000)
SQLALCHEMY_BASE = declarative_base()
SQLALCHEMY_BASE.metadata.create_all(SQLALCHEMY_ENGINE)

import os
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
                 gcp_directory,
                 extract_directory,
                 transform_directory,
                 transformer=DummyTransformer(),
                 loader=DummyLoader()):
        self._name = name
        self._source_directory = gcp_directory
        self._extract_directory = extract_directory
        self._transform_directory = transform_directory
        self._transformer = transformer
        self._loader = loader

    @property
    def name(self):
        return self._name

    @property
    def gcp_directory(self):
        return self._source_directory

    @property
    def extract_directory(self):
        return self._extract_directory

    @property
    def transform_directory(self):
        return self._transform_directory

    @property
    def transformer(self):
        return self._transformer

    @property
    def loader(self):
        return self._loader


DEBUG = 1

ETL_BASE_DIRECTORY = Path.cwd() / 'static' / 'etl'
EXTRACT_DIRECTORY = ETL_BASE_DIRECTORY / 'extract'
TRANSFORM_DIRECTORY = ETL_BASE_DIRECTORY / 'transform'

# noinspection PyTypeChecker
ETL_CONFIG_ITEMS = [ETLConfigItem(name='Nationale databank flora en fauna',
                                  gcp_directory='NDFF',
                                  extract_directory=EXTRACT_DIRECTORY / 'NDFF',
                                  transform_directory=TRANSFORM_DIRECTORY / 'NDFF'),
                    ETLConfigItem(name='Koninkelijk nationaal metreologisch instituut',
                                  gcp_directory='KNMI',
                                  extract_directory=EXTRACT_DIRECTORY / 'KNMI',
                                  transform_directory=TRANSFORM_DIRECTORY / 'KNMI',
                                  transformer=KNMITransformer(),
                                  loader=KNMILoader()),
                    ETLConfigItem(name='Townships',
                                  gcp_directory='Townships',
                                  extract_directory=EXTRACT_DIRECTORY / 'townships',
                                  transformer=TownshipTransformer(),
                                  transform_directory=TRANSFORM_DIRECTORY / 'townships',
                                  loader=TownshipLoader()),
                    ETLConfigItem(name='Boomregister',
                                  gcp_directory='Boomregister',
                                  extract_directory=EXTRACT_DIRECTORY / 'Boomregister',
                                  transform_directory=TRANSFORM_DIRECTORY / 'Boomregister')
                    ]

# Set google cloud config
GCP_BUCKET = 'vaa-opm'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(
    Path.cwd() / 'static' / 'secrets' / 'cloud_storage_key.json')  # Authentication

# Set postresql/postgis config
SQLALCHEMY_ENGINE = create_engine('postgresql://tim:doyouopm@localhost:5432/opm', echo=DEBUG)
SQLALCHEMY_BASE = declarative_base()
SQLALCHEMY_BASE.metadata.create_all(SQLALCHEMY_ENGINE)

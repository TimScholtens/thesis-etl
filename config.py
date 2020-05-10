import os
from pathlib import Path
from sqlalchemy import create_engine
from etl.transform.transformers.township import Township as TownshipTransformer
from etl.transform.transformers.dummy import Dummy as DummyTransformer


class ETLConfigItem:

    def __init__(self, name, source_directory, destination_directory, transformer=DummyTransformer()):
        self._name = name
        self._source_directory = source_directory
        self._destination_directory = destination_directory
        self._transformer = transformer

    @property
    def name(self):
        return self._name

    @property
    def source_directory(self):
        return self._source_directory

    @property
    def destination_directory(self):
        return self._destination_directory

    @property
    def transformer(self):
        return self._transformer


DESTINATION_DIRECTORY = Path.cwd() / 'static' / 'etl' / 'extract'

ETL_CONFIG_ITEMS = [ETLConfigItem(name='Nationale databank flora en fauna',
                                  source_directory='NDFF',
                                  destination_directory=DESTINATION_DIRECTORY / 'NDFF'),
                    ETLConfigItem(name='Koninkelijk nationaal metreologisch instituut',
                                  source_directory='KNMI',
                                  destination_directory=DESTINATION_DIRECTORY / 'KNMI'),
                    ETLConfigItem(name='Townships',
                                  source_directory='Townships',
                                  destination_directory=DESTINATION_DIRECTORY / 'townships',
                                  transformer=TownshipTransformer()),
                    ETLConfigItem(name='Boomregister',
                                  source_directory='Boomregister',
                                  destination_directory=DESTINATION_DIRECTORY / 'Boomregister')
                    ]

# Set google cloud config
GCP_BUCKET = 'vaa-opm'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(
    Path.cwd() / 'static' / 'secrets' / 'cloud_storage_key.json')  # Authentication

# SQLALCHEMY_ENGINE = create_engine("mssql+pymssql://SA:Password1!@localhost")

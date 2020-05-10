import os
from pathlib import Path
from sqlalchemy import create_engine

class Extract:
    class ExtractConfigItem:

        def __init__(self, name, source_directory, destination_directory, ignore=''):
            self._name = name
            self._source_directory = source_directory
            self._destination_directory = destination_directory
            self._ignore = ignore

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
        def ignore(self):
            return self._ignore

    DESTINATION_DIRECTORY = Path.cwd() / 'static'

    DIRECTORIES_TO_EXTRACT = [ExtractConfigItem(name='Nationale databank flora en fauna',
                                                source_directory='NDFF',
                                                destination_directory=DESTINATION_DIRECTORY / 'NDFF'),
                              ExtractConfigItem(name='Koninkelijk nationaal metreologisch instituut',
                                                source_directory='KNMI',
                                                destination_directory=DESTINATION_DIRECTORY / 'KNMI'),
                              ExtractConfigItem(name='Townships',
                                                source_directory='Townships',
                                                destination_directory=DESTINATION_DIRECTORY / 'townships'),
                              ExtractConfigItem(name='Boomregister',
                                                source_directory='Boomregister',
                                                destination_directory=DESTINATION_DIRECTORY / 'Boomregister')
                              ]

    # Set google cloud config
    GCP_BUCKET = 'vaa-opm'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(Path.cwd() / 'static' / 'secrets' / 'cloud_storage_key.json') # Authentication

class Transform:
    pass

class Load:

    SQLALCHEMY_ENGINE = create_engine("mssql+pymssql://SA:Password1!@localhost")






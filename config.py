import os
from pathlib import Path


class Extract:
    class ExtractItem:

        def __init__(self, name, source_directory, destination_directory):
            self._name = name
            self._source_directory = source_directory
            self._destination_directory = destination_directory

        @property
        def name(self):
            return self._name

        @property
        def source_directory(self):
            return self._source_directory

        @property
        def destination_directory(self):
            return self._destination_directory

    DESTINATION_FOLDER = Path.cwd() / 'static'
    FOLDERS_TO_EXTRACT = [ExtractItem(name='Nationale databank flora en fauna', source_directory='NDFF', destination_directory=DESTINATION_FOLDER),
                          ExtractItem(name='Koninkelijk nationaal metreologisch instituut', source_directory='KNMI', destination_directory=DESTINATION_FOLDER),
                          ExtractItem(name='Townships', source_directory='townships', destination_directory=DESTINATION_FOLDER)]

    # Set google cloud config
    GCP_BUCKET = 'vaa-opm'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(Path.cwd() / 'static' / 'secrets' / 'cloud_storage_key.json') # Authentication

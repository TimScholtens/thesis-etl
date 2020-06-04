# Load data from gcloud storage
from google.cloud import storage
from pathlib import Path


def download_uris(gs_uris, destination_location):
    """
    Downloads all files within the directory located within google cloud storage

    :param gs_uris:  list of gs_uris which should be downloaded. gs://bucket_name/path/to/blob ->
    eg. [gs://vaa-opm/knmi/weather_station_data.json, gs://vaa-opm/knmi/weather_station_locations.json']
    :param destination_location: directory on local machine where downloaded files should be saved.

    More information: https://googleapis.dev/python/storage/latest/client.html
    """

    # Create local directory if not exists
    if not Path(destination_location).is_dir():
        Path.mkdir(destination_location, parents=True, exist_ok=True)

    for gs_uri in gs_uris:
        uri = _GoogleStorageURI(uri=gs_uri)
        print(f'Starting download of {uri.file_name}')

        source_file_name = uri.path
        destination_file_name = Path(destination_location) / uri.file_name
        download_file(bucket=uri.bucket,
                      source_file_name=source_file_name,
                      destination_file_name=destination_file_name)


def download_file(bucket, source_file_name, destination_file_name):
    """
    :param bucket: google storage lango for repository.
    :param source_file_name: absolute path within google storage.
    :param destination_file_name: absolute path on local machine.

    More information: https://googleapis.dev/python/storage/latest/client.html
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = storage.Blob(source_file_name, bucket)
    blob.download_to_filename(destination_file_name)

    print(f"File {source_file_name} downloaded to {destination_file_name}.")


class _GoogleStorageURI:

    def __init__(self, uri):
        self._uri = uri
        self._bucket = uri.split('/')[2]
        self._path = str.join('/', uri.split('/')[3:])
        self._file_name = uri.split('/')[-1]

    @property
    def uri(self):
        return self._uri

    @property
    def bucket(self):
        return self._bucket

    @property
    def file_name(self):
        return self._file_name

    @property
    def path(self):
        return self._path

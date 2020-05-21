# Load data from gcloud storage
from google.cloud import storage
from pathlib import Path


def download_directory(bucket_name, source_location, destination_location):
    """
    Downloads all files within the directory located within google cloud storage

    :param bucket_name: google cloud storage specific lango for repository name.
    :param source_location:  directory within google cloud storage which files should be downloaded.
    :param destination_location: directory on local machine where downloaded files should be saved.
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=source_location)

    # Create local directory if not exists
    if not Path(destination_location).is_dir():
        Path.mkdir(destination_location)

    for blob in blobs:
        file_name = blob.name.split('/')[-1]
        print(f'Starting download of {file_name}')

        source_file_name = blob.name
        destination_file_name = Path(destination_location) / file_name
        download_file(bucket_name=bucket_name, source_file_name=source_file_name, destination_file_name=destination_file_name)


def download_uris(bucket_name, gcp_uris, destination_location):
    """
    Downloads all files within the directory located within google cloud storage

    :param bucket_name: google cloud storage specific lango for repository name.
    :param gcp_uris:  list of gcp_uris which should be downloaded. eg. [/vaa-opm/knmi/weather_station_data.json,
    /vaa-opm/knmi/weather_station_locations.json']
    :param destination_location: directory on local machine where downloaded files should be saved.
    """

    # Create local directory if not exists
    if not Path(destination_location).is_dir():
        Path.mkdir(destination_location)

    for gcp_uri in gcp_uris:
        file_name = gcp_uri.name.split('/')[-1]
        print(f'Starting download of {file_name}')

        source_file_name = gcp_uri
        destination_file_name = Path(destination_location) / file_name
        download_file(bucket_name=bucket_name,
                      source_file_name=source_file_name,
                      destination_file_name=destination_file_name)



def download_file(source_file_name, destination_file_name):
    """Downloads a file from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"
    storage_client = storage.Client()

    with open('file-to-download-to') as file_obj:
        client.download_blob_to_file(
            f'gs://{bucket_name}{source_file_name}', file_obj)


    blob.download_to_filename(destination_file_name)

    print(f"Blob {source_file_name} downloaded to {destination_file_name}.")





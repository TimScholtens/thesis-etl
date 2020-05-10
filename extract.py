# Load data from gcloud storage
from google.cloud import storage
from pathlib import Path


def download_directory(bucket_name, source_directory_name, destination_folder_name):
    """Downloads a blob containing other blobs from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_directory = "local/path/to/directory"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=source_directory_name)

    # Create local directory if not exists
    if not Path(destination_folder_name).is_dir():
        Path.mkdir(destination_folder_name)

    for blob in blobs:
        file_name = blob.name.split('/')[1]
        print(f'Starting download of {file_name}')

        source_file_name = blob.name
        destination_file_name = Path(destination_folder_name) / file_name
        download_file(bucket_name=bucket_name, source_file_name=source_file_name, destination_file_name=destination_file_name)


def download_file(bucket_name, source_file_name, destination_file_name):
    """Downloads a file from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)
    blob.download_to_filename(destination_file_name)

    print(f"Blob {source_file_name} downloaded to {destination_file_name}.")





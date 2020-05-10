import config
from extract import download_directory
from load import insert_townships

def extract_all_data():
    print("Start extracting all data...")

    for directory in config.Extract.DIRECTORIES_TO_EXTRACT:
        download_directory(bucket_name=config.Extract.GCP_BUCKET,
                           source_directory_name=directory.source_directory,
                           destination_folder_name=directory.destination_directory)


def transform_all_data():
    pass


def load_all_data():

    # Townships
    insert_townships()




extract_all_data()

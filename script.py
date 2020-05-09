import config
from extract import download_blobs

def extract_all_data():
    print("Start extracting all data...")
    # Loop through config file

    for folder in config.Extract.FOLDERS_TO_EXTRACT:

        download_blobs(bucket_name=config.Extract.GCP_BUCKET, source_blob_name=)



def transform_all_data():
    pass

def load_all_data():
    pass
import config
from etl.extract.gcp import download_directory
from etl.load.township import insert_townships
from etl.transform.transformer import  transform


def extract_all_data():
    print("Start extracting all data...")

    for etl_config_item in config.ETL_CONFIG_ITEMS:
        download_directory(bucket_name=config.GCP_BUCKET,
                           source_directory_name=etl_config_item.source_directory,
                           destination_folder_name=etl_config_item.destination_directory)


def transform_all_data():
    print("Start transforming all data...")

    for etl_config_item in config.ETL_CONFIG_ITEMS:
        transform(transformer=etl_config_item.transformer, extract_directory=etl_config_item.extract_directory)



def load_all_data():
    # Townships
    insert_townships()


# extract_all_data()
transform_all_data()
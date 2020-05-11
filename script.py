import config
from etl.load.models import *
from etl.extract.gcp import download_directory
from etl.transform.transformer import transform
from etl.load.loader import load


def extract_all_data():
    print("Start extracting all data...")

    for etl_config_item in config.ETL_CONFIG_ITEMS:
        download_directory(bucket_name=config.GCP_BUCKET,
                           source_directory_name=etl_config_item.source_directory,
                           destination_folder_name=etl_config_item.destination_directory)


def transform_all_data():
    print("Start transforming all data...")

    for etl_config_item in config.ETL_CONFIG_ITEMS:
        transform(transformer=etl_config_item.transformer,
                  extract_directory=etl_config_item.extract_directory,
                  transform_directory=etl_config_item.transform_directory)


def load_all_data():
    print("Start loading all data...")

    # If tables don't exist yet in the database, create them
    config.SQLALCHEMY_BASE.metadata.create_all(config.SQLALCHEMY_ENGINE, checkfirst=False)

    for etl_config_item in config.ETL_CONFIG_ITEMS:
        load(etl_config_item.loader, transform_directory=etl_config_item.transform_directory)


# extract_all_data()
# transform_all_data()
load_all_data()

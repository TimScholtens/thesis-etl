import config
from etl.load.models import *  # required for creating models in database
from etl.extract.gcp import download_uris
from etl.transform.transformer import transform
from etl.load.loader import load


def extract_all_data():
    print("Start extracting all data...")

    for etl_config_item in config.ETL_CONFIG_ITEMS:
        download_uris(gs_uris=etl_config_item.gs_uris,
                      destination_location=etl_config_item.extract_location)


def transform_all_data():
    print("Start transforming all data...")

    for etl_config_item in config.ETL_CONFIG_ITEMS:
        transform(transformer=etl_config_item.transformer,
                  extract_directory=etl_config_item.extract_location,
                  transform_directory=etl_config_item.transform_location)


def load_all_data():
    print("Start loading all data...")

    # If tables don't exist yet in the database, create them
    config.SQLALCHEMY_BASE.metadata.create_all(config.SQLALCHEMY_ENGINE, checkfirst=True)

    for etl_config_item in config.ETL_CONFIG_ITEMS:
        load(etl_config_item.loader, transform_directory=etl_config_item.transform_location)


# extract_all_data()
# transform_all_data()
load_all_data()

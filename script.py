import config
from etl.load.models import *  # required for creating models in database
from etl.extract.gcp import download_uris
from etl.transform.transformer import transform
from etl.load.loader import load
from etl.jobs import ETL_JOBS


def extract_all_data():
    print("Start extracting all data...")

    for etl_job in ETL_JOBS:
        download_uris(gs_uris=etl_job.gs_uris,
                      destination_location=etl_job.extract_location)


def transform_all_data():
    print("Start transforming all data...")

    for etl_job in ETL_JOBS:
        transform(transformer=etl_job.transformer,
                  extract_directory=etl_job.extract_location,
                  transform_directory=etl_job.transform_location)


def load_all_data():
    print("Start loading all data...")

    # If tables don't exist yet in the database, create them
    config.SQLALCHEMY_BASE.metadata.create_all(config.SQLALCHEMY_ENGINE, checkfirst=True)

    for etl_job in ETL_JOBS:
        load(etl_job.loader, transform_directory=etl_job.transform_location)


# extract_all_data()
transform_all_data()
# load_all_data()

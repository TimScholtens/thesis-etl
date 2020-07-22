import os
import csv
import ctypes
from decimal import getcontext
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

# Debug config
DEBUG = 1

# Set decimal precision
getcontext().prec = 2

# Set CSV max field size
csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))  # max 32bit integer value

# Final transformation ID
FINAL_TRANSFORMATION_ID = 'FINAL'

# Set google cloud config
GCP_BUCKET = 'vaa-opm'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(
    Path.cwd() / 'static' / 'secrets' / 'cloud_storage_key.json')  # Authentication

# Set postresql/postgis config
SQLALCHEMY_ENGINE = create_engine('postgres://tim:doyouopm@localhost:5432/opm',
                                  echo=DEBUG,
                                  executemany_mode='values',
                                  executemany_values_page_size=10000,
                                  client_encoding='utf8')
SQLALCHEMY_BASE = declarative_base()

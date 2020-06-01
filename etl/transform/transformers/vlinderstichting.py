from etl.transform.transformers.base import Base
from pathlib import Path
from datetime import datetime
import pandas as pd


class Vlinderstichting(Base):

    def transform(self, extract_directory, transform_directory):
        column_mapping = {
            "dag": "day",
            "maand": "month",
            "jaar": "year",
            "stadium": "stage",
            "x": "longitude",
            "y": "latitude"
        }

        dtypes = {
            "day": "uint8",
            "month": "uint8",
            "year": "uint16",
            "stage": "categorical",
            "longitude": "float32",
            "latitude": "float32"
        }

        file_path = extract_directory / 'epr_20200521.csv'
        df = pd.read_csv(
            file_path,
            usecols=list(column_mapping),
            dtype=dtypes,
            header=0
        )

        # Set date column
        df['date'] = pd.to_datetime(df[['year', 'month', 'day']])

        print(df)

        # Convert "rijksdriehoekcoordinaten" EPSG 28992 to EPSG 4326 (WSG 84)



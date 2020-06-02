from etl.transform.transformers.base import Base
from pathlib import Path
import pandas as pd
from pyproj import Transformer
from shapely.geometry import Point

class Vlinderstichting(Base):

    def transform(self, extract_directory, transform_directory):
        from config import FINAL_TRANSFORMATION_ID

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
            "stage": "category",
            "longitude": "float32",
            "latitude": "float32"
        }

        file_path = extract_directory / 'epr_20200521.csv'
        df = pd.read_csv(
            file_path,
            usecols=list(column_mapping),
            dtype=dtypes,
            header=0,
            sep=';'
        )

        # Rename column names
        df = df.rename(columns=column_mapping)

        # Set date column
        df['date'] = pd.to_datetime(df[['year', 'month', 'day']])

        # Convert  EPSG 28992 ("rijksdriehoekcoordinaten") to EPSG 4326 (WSG 84)
        transformer = Transformer.from_crs(28992, 4326, always_xy=True)
        df['geometry'] = df.apply(lambda row: Point(transformer.transform(row['longitude'], row['latitude'])), axis=1) # in wkt format by default

        # Filter and save as csv
        if not Path(transform_directory).is_dir():
            Path.mkdir(transform_directory)

        df[['date', 'stage', 'geometry']].to_csv(
            transform_directory / f'Vlinderstichting_{FINAL_TRANSFORMATION_ID}.csv', index=False)

import pandas as pd
from etl.transform.transformers.base import Base
from pathlib import Path
from pyproj import Transformer
from shapely.geometry import Point
from config import FINAL_TRANSFORMATION_ID


class TreeAmsterdam(Base):

    def transform(self, extract_directory, transform_directory):
        column_mapping = {
            'X': 'longitude',
            'Y': 'latitude',
            'Boomsoort': 'species_latin',
            'Boomsoort nl': 'species_dutch'
        }

        dtypes = {
            "X": "float32",
            "Y": "float32",
            "Boomsoort": "str",
            "Boomsoort nl": "str",
        }

        file_path = extract_directory / 'bomenbestand.csv'

        df = pd.read_csv(
            file_path,
            usecols=list(column_mapping),
            dtype=dtypes,
            header=0
        )

        # Rename column names
        df = df.rename(columns=column_mapping)

        # Convert  EPSG 28992 ("rijksdriehoekcoordinaten") to EPSG 4326 (WSG 84)
        transformer = Transformer.from_crs(28992, 4326, always_xy=True)
        df['geometry'] = df.apply(lambda row: Point(transformer.transform(row['longitude'], row['latitude'])), axis=1) # in wkt format by default

        # Save as csv
        if not Path(transform_directory).is_dir():
            Path.mkdir(transform_directory, parents=True, exist_ok=True)

        df.to_csv(transform_directory / f'Tree_Amsterdam_{FINAL_TRANSFORMATION_ID}.csv', index=False, na_rep='')
import pandas as pd
import csv
import geopandas as gpd
import shapely.wkt
from etl.transform.transformers.base import Base
from pathlib import Path
from config import FINAL_TRANSFORMATION_ID


class WURAlterra(Base):

    def transform(self, extract_directory, transform_directory):
        column_mapping = {
            'geometry': 'geometry',
            'OMSCHRIJVI': 'soil_type',
            'date': 'date'
        }

        dtypes = {
            "geometry": "str",
            "OMSCHRIJVI": "str",
            "date": "str",
        }

        file_path = extract_directory / 'bodemkaart.csv'

        df = pd.read_csv(
            file_path,
            usecols=list(column_mapping),
            dtype=dtypes,
            header=0
        )

        # Rename column names
        df = df.rename(columns=column_mapping)

        # Convert  EPSG 28992 ("rijksdriehoekcoordinaten") to EPSG 4326 (WSG 84)
        gdf = gpd.GeoDataFrame(df, crs={'init': 'epsg:28992'})
        gdf['geometry'] = gdf['geometry'].apply(shapely.wkt.loads)
        gdf = gdf.to_crs({'init': 'epsg:4326'})

        # Save as csv
        if not Path(transform_directory).is_dir():
            Path.mkdir(transform_directory, parents=True, exist_ok=True)

        gdf.to_csv(transform_directory / f'bodemkaart_{FINAL_TRANSFORMATION_ID}.csv', index=False, na_rep='',
                   quoting=csv.QUOTE_ALL)

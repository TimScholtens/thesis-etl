import pandas as pd
from etl.transform.transformers.base import Base
from pathlib import Path
from config import FINAL_TRANSFORMATION_ID

class KNMIWeatherStationData(Base):

    def transform(self, extract_directory, transform_directory):

        # Rename to more readable names,
        # note: only select columns which are related to BIOCLIM, being temperature and perception
        column_mapping = {
            'STN': 'station_id',
            'YYYYMMDD': 'date',
            'TG': 'temperature_avg',
            'TN': 'temperature_min',
            'TX': 'temperature_max',
            'SQ': 'sunshine_duration',
            'Q': 'sunshine_radiation',
            'DR': 'rain_duration',
            'RH': 'rain_sum',
            'UG': 'humidity_avg',
            'UX': 'humidity_max',
            'UN': 'humidity_min'
        }

        dtypes = {
            "STN": "uint16",
            "YYYYMMDD": "str",
            "TG": "float32",
            "TN": "float32",
            "TX": "float32",
            "SQ": "float32",
            "Q": "float32",
            "DR": "float32",
            "RH": "float32",
            "UG": "float32",
            "UX": "float32",
            "UN": "float32"
        }

        # Load
        df_weather_station_data = pd.read_csv(
            extract_directory / 'station_data.csv',
            dtype=dtypes,
            usecols=list(column_mapping),
            header=40
        )

        # Rename to more meaningful names
        df_weather_station_data = df_weather_station_data.rename(columns=column_mapping)

        # Set datetimeindex
        df_weather_station_data['date'] = pd.to_datetime(df_weather_station_data['date'])

        # Transform temperature, sunshine and rain to decimal values (check description of this function)
        df_weather_station_data[
            ['temperature_avg',
             'temperature_min',
             'temperature_max',
             'sunshine_duration',
             'sunshine_radiation',
             'rain_duration']
        ] = df_weather_station_data[
                ['temperature_avg',
                 'temperature_min',
                 'temperature_max',
                 'sunshine_duration',
                 'sunshine_radiation',
                 'rain_duration']] / 10

        # Set output file
        final_file_name = f'station_data_{FINAL_TRANSFORMATION_ID}.csv'
        output_file_path = transform_directory / final_file_name

        # Create local directory if not exists
        if not Path(transform_directory).is_dir():
            Path.mkdir(transform_directory, parents=True, exist_ok=True)

        # Write transformations to file
        df_weather_station_data.to_csv(output_file_path, index=False, na_rep='nan')

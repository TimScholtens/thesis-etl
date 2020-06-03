from etl.transform.transformers.base import Base
import pandas as pd
from pathlib import Path


class KNMIWeatherStationData(Base):

    def transform(self, extract_directory, transform_directory):
        import config

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
            "station_id": "uint16", # STN
            "temperature_avg": "float32", # TG
            "temperature_min": "float32", # TN
            "temperature_max": "float32", # TX
            "sunshine_duration": "float32", # SQ
            "sunshine_radiation": "float32", # Q
            "rain_duration": "float32", # Q
            "rain_sum": "float32", # RH
            "humidity_avg": "float32", # UG
            "humidity_max": "float32", # UX
            "humidity_min": "float32" # UN
        }

        # Load
        df_weather_station_data = pd.read_csv(
            extract_directory / 'station_data.csv',
            # parse_dates=['date'],
            dtype=dtypes,
            usecols=list(dtypes) + ["date"],
            names=list(dtypes) + ["date"],
            header=52  # first 51 rows consist of documentation
        )

        df_weather_station_data['date'] = pd.to_datetime(df_weather_station_data['date'])

        # Transform temperature, sunshine and rain to decimal values (check documentation of )
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
        final_file_name = f'station_data_{config.FINAL_TRANSFORMATION_ID}.csv'
        output_file_path = transform_directory / final_file_name

        # Create local directory if not exists
        if not Path(transform_directory).is_dir():
            Path.mkdir(transform_directory)

        # Write transformations to file
        df_weather_station_data.to_csv(output_file_path, index=False)

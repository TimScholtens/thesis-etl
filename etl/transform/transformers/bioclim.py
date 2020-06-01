from etl.transform.transformers.base import Base
from pathlib import Path
from sklearn.neighbors import BallTree, KNeighborsRegressor
import json
import geopandas as gpd
import pandas as pd


def save_dataframe_to_csv(path, dataframe):
    # Create local directory if not exists
    if not path.parent.is_dir():
        Path.mkdir(path.parent)

    dataframe.to_csv(path, index=False)


def load_weather_station_locations(extract_directory):
    # Rename to more readable names,
    # note: only select columns which are related to BIOCLIM being temperature and perception
    column_mapping = {
        'STN': 'station_id',
        'LON(east)': 'longitude',
        'LAT(north)': 'latitude',
        'ALT(m)': 'altitude',
        'NAME': 'name'
    }

    dtypes = {
        "station_id": "uint16",
        "longitude": "float32",
        "latitude": "float32",
        "altitude": "float32",
        "name": "str",
    }

    df_weather_station_location = pd.read_csv(
        extract_directory / 'station_locations.csv',
        dtype=dtypes,
        usecols=list(dtypes),
        names=list(dtypes),
        header=0
    )

    return df_weather_station_location


def load_weather_station_data(extract_directory):

    # Load
    df_weather_station_data = gpd.read_file(extract_directory / 'station_data.csv')

    # Rename to more readable names,
    # note: only select columns which are related to BIOCLIM being temperature and perception
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
        'UN': 'humidity_min'}

    # Filter columns
    columns_of_interest = [column for column in column_mapping.keys()]
    df_weather_station_data = df_weather_station_data[columns_of_interest]

    # Rename
    df_weather_station_data = df_weather_station_data.rename(columns=column_mapping)

    # Set data types
    df_weather_station_data['station_id'] = pd.to_numeric(df_weather_station_data['station_id'], errors='coerce')
    df_weather_station_data['date'] = pd.to_datetime(df_weather_station_data['date'])
    df_weather_station_data['temperature_avg'] = pd.to_numeric(df_weather_station_data['temperature_avg'],
                                                               errors='coerce')
    df_weather_station_data['temperature_min'] = pd.to_numeric(df_weather_station_data['temperature_min'],
                                                               errors='coerce')
    df_weather_station_data['temperature_max'] = pd.to_numeric(df_weather_station_data['temperature_max'],
                                                               errors='coerce')
    df_weather_station_data['sunshine_duration'] = pd.to_numeric(df_weather_station_data['sunshine_duration'],
                                                                 errors='coerce')
    df_weather_station_data['sunshine_radiation'] = pd.to_numeric(df_weather_station_data['sunshine_radiation'],
                                                                  errors='coerce')
    df_weather_station_data['rain_duration'] = pd.to_numeric(df_weather_station_data['rain_duration'], errors='coerce')
    df_weather_station_data['rain_sum'] = pd.to_numeric(df_weather_station_data['rain_sum'], errors='coerce')
    df_weather_station_data['humidity_avg'] = pd.to_numeric(df_weather_station_data['humidity_avg'], errors='coerce')
    df_weather_station_data['humidity_max'] = pd.to_numeric(df_weather_station_data['humidity_max'], errors='coerce')

    # Transform temperature, sunshine and rain to decimal values (check description of this function)
    df_weather_station_data['temperature_avg'] = df_weather_station_data['temperature_avg'] / 10
    df_weather_station_data['temperature_min'] = df_weather_station_data['temperature_min'] / 10
    df_weather_station_data['temperature_max'] = df_weather_station_data['temperature_max'] / 10
    df_weather_station_data['sunshine_duration'] = df_weather_station_data['sunshine_duration'] / 10
    df_weather_station_data['sunshine_radiation'] = df_weather_station_data['sunshine_radiation'] / 10
    df_weather_station_data['rain_duration'] = df_weather_station_data['rain_duration'] / 10

    return df_weather_station_data


def load_township_data(extract_directory):
    return gpd.read_file(extract_directory / 'townships.json')


class BioClim_1(Base):

    def transform(self, extract_directory, transform_directory):
        from config import FINAL_TRANSFORMATION_ID

        def calculate_townships_centroids(df_township):
            # Get centroids
            df_township_centroid = gpd.GeoDataFrame(df_township.centroid, columns=['centroid'], geometry='centroid')

            # Concat
            df_township_centroid = pd.concat([df_township_centroid, df_township[['code', 'name']]], axis=1)

            return df_township_centroid

        def yearly_avg_temperature_weather_station_data(weather_station_data):
            # Todo: dirty code, refactor
            agg_data = weather_station_data.groupby(['station_id', pd.Grouper(key='date', freq='Y')]).mean()
            agg_data = agg_data.reset_index()
            agg_data['year'] = agg_data['date'].dt.year

            return agg_data  # Todo: No clue how this works

        def interpolate(df_townships_centroids, ws_yearly_average_temperature, ws_location):
            """
            Fit k-nearest-neighbors ball tree having parameters:
             - For interpolation use Inverse Distance Weighting to determine weights of each weather station
             - For interpolation use Haversine to calculate distance between given point and weather station
            """

            # Loop through each year and fit KNN tree,
            # then calculate for each township centroid the interpolated temperature for that year.
            # return -> township | year | temperature

            # Create new dataframe for holding interpolated temperatures
            df_townships_interpolated = pd.DataFrame(columns=['township', 'interpolated_temperature', 'year'])

            for year in set(ws_yearly_average_temperature['year'].values):
                # Get ws coordinates and temperature for given year
                year_filter = ws_yearly_average_temperature['year'] == year
                ws_temperature = ws_yearly_average_temperature.where(year_filter).dropna()
                ws_temperature_location = ws_temperature.merge(ws_location, left_on='station_id', right_on='station_id')

                # Fit tree
                ws_coords = ws_temperature_location[['longitude', 'latitude']].values  # X
                ws_temperatures = ws_temperature_location['temperature_avg'].values  # y
                knn_regressor = KNeighborsRegressor(metric='haversine', algorithm='ball_tree', weights='distance',
                                                    leaf_size=2)
                knn_regressor.fit(ws_coords, ws_temperatures)

                # Calculate interpolated temperature for each township centroid
                def interpolate_temperature(point):
                    latitude = point.x
                    longitude = point.y

                    return knn_regressor.predict([[latitude, longitude]])[0]

                interpolated_townships = [dict(year=year,
                                               township=row['name'],
                                               interpolated_temperature=interpolate_temperature(row['centroid']))
                                          for index, row in df_townships_centroids.iterrows()]

                df_townships_interpolated = df_townships_interpolated.append(interpolated_townships)

            return df_townships_interpolated

        # Load data
        weather_station_data = load_weather_station_data(extract_directory)
        weather_station_locations = load_weather_station_locations(extract_directory)
        townships = load_township_data(extract_directory)

        # Calculate centroids for each township, save output
        df_townships_centroids = calculate_townships_centroids(townships)
        save_dataframe_to_csv(path=transform_directory / 'township_centroids.csv', dataframe=df_townships_centroids)

        # Filter columns for each weather station
        df_ws_avg_temperature = weather_station_data[['temperature_avg', 'station_id', 'date']]

        # Filter out temperature records having no value
        df_ws_avg_temperature = df_ws_avg_temperature.dropna()

        # Calculate yearly average temperature for each weather station, save output
        df_ws_yearly_avg_temperature = yearly_avg_temperature_weather_station_data(df_ws_avg_temperature)
        save_dataframe_to_csv(path=transform_directory / 'weather_station_avg_temperature.csv',
                              dataframe=df_ws_yearly_avg_temperature)

        # Interpolate for each township the annual temperature, save output "FINAL"
        df_townships_interpolated = interpolate(ws_yearly_average_temperature=df_ws_yearly_avg_temperature,
                                                ws_location=weather_station_locations,
                                                df_townships_centroids=df_townships_centroids)

        save_dataframe_to_csv(
            path=transform_directory / f'township_interpolated_temperatures_{FINAL_TRANSFORMATION_ID}.csv',
            dataframe=df_townships_interpolated)


class BioClim_2(Base):
    pass

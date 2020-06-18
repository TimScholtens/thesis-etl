import numpy as np
import geopandas as gpd
import pandas as pd
from etl.transform.transformers.base import Base
from pathlib import Path
from sklearn.neighbors import KNeighborsRegressor
from abc import ABC, abstractmethod
from config import FINAL_TRANSFORMATION_ID


def save_dataframe_to_csv(path, dataframe):
    # Create local directory if not exists
    if not path.parent.is_dir():
        Path.mkdir(path.parent, parents=True, exist_ok=True)

    dataframe.to_csv(path, index=False)


def load_weather_station_data(extract_directory):
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

    # Replace -1 values for 'rain_sum' and 'sun_duration' with 0
    df_weather_station_data[['rain_sum', 'rain_duration']] = df_weather_station_data[
        ['rain_sum', 'rain_duration']].replace(-1, 0)

    # Transform temperature, sunshine and rain to decimal values (check description of this function)
    df_weather_station_data[
        ['temperature_avg',
         'temperature_min',
         'temperature_max',
         'sunshine_duration',
         'sunshine_radiation',
         'rain_duration',
         'rain_sum']
    ] = df_weather_station_data[
            ['temperature_avg',
             'temperature_min',
             'temperature_max',
             'sunshine_duration',
             'sunshine_radiation',
             'rain_duration',
             'rain_sum']] / 10

    return df_weather_station_data


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
        usecols=list(column_mapping),
        header=0
    )

    # Rename to more meaningful names
    df_weather_station_location = df_weather_station_location.rename(columns=column_mapping)

    return df_weather_station_location


def load_township_data(extract_directory):
    return gpd.read_file(extract_directory / 'townships.json')


def calculate_townships_centroids(df_township):
    # Get centroids
    df_township_centroid = gpd.GeoDataFrame(df_township.centroid, columns=['centroid'], geometry='centroid')

    # Concat
    df_township_centroid = pd.concat([df_township_centroid, df_township[['code', 'name']]], axis=1)

    return df_township_centroid


def interpolate(X, X_interpolate_locations, y):
    """
    X: training points
    X_interpolate: points which have to be interpolated
    y: y training values

    returns: numpy array holding all predicted values
    """
    knn_regressor = KNeighborsRegressor(metric='haversine', algorithm='ball_tree', weights='distance', leaf_size=2)
    knn_regressor.fit(X, y)

    return knn_regressor.predict(X_interpolate_locations)


class BioClim(Base, ABC):

    def __init__(self):
        self._X_interpolate_labels = None
        self._X_interpolate_locations = None

    def load_extract_files(self, extract_directory, transform_directory):
        # Merge weather station data and their locations
        weather_station_data = load_weather_station_data(extract_directory)
        weather_station_locations = load_weather_station_locations(extract_directory)

        self.weather_station_data_and_locations = weather_station_data.merge(weather_station_locations,
                                                                             left_on='station_id',
                                                                             right_on='station_id')

        # Set townships
        self.townships = load_township_data(extract_directory)

    @property
    def X_interpolate_locations(self):
        """
            Points (lon,lat) which have to be interpolated.
            returns numpy array containing all long, lat coordinates which have to be interpolated.
        """

        if self._X_interpolate_locations is None:
            # Calculate centroid
            df_townships_centroids = calculate_townships_centroids(self.townships)

            df_townships_centroids['longitude'] = df_townships_centroids['centroid'].apply(lambda point: point.x)
            df_townships_centroids['latitude'] = df_townships_centroids['centroid'].apply(lambda point: point.y)

            self._X_interpolate_locations = df_townships_centroids[['longitude', 'latitude']].values

        return self._X_interpolate_locations

    @property
    def X_interpolate_labels(self):
        """
            Labels of points which have to be interpolated.
            returns numpy array containing all point labels.
        """

        if self._X_interpolate_labels is None:
            self._X_interpolate_labels = self.townships[['name']].values

        return self._X_interpolate_labels[:, 0]

    def X(self, dataframe):
        """
            Returns known points.
            return numpy array ([long,lat]) of known points
        """
        return dataframe[['longitude', 'latitude']].values

    @abstractmethod
    def y(self, dataframe):
        """
            Different per BIOCLIM variable; training data response variable
        """
        pass

    @abstractmethod
    def aggregate(self, dataframe):
        """
            Different per BIOCLIM variable; how the data should be partitioned and which arithmetic.
        """

    def training_data(self):
        """
            Returns a generator which partitions the data supplied by the 'aggregate' function.
        """

        grouped_training_data = self.aggregate(self.weather_station_data_and_locations)
        time_windows = set([index[0] for index in grouped_training_data.index])

        for time_window in time_windows:
            df_window = grouped_training_data.loc[(time_window,)]
            X = self.X(df_window)
            y = self.y(df_window)

            # Filter out NaN values
            non_nan_indexes = np.where(~np.isnan(y))[0]
            X = X[non_nan_indexes]
            y = y[non_nan_indexes]

            yield (X, y), time_window

    def transform(self, extract_directory, transform_directory):
        # Load required files
        self.load_extract_files(extract_directory=extract_directory, transform_directory=transform_directory)

        # Interpolate for each township the annual temperature, save output "FINAL"
        X_interpolate_locations = self.X_interpolate_locations
        X_interpolate_labels = self.X_interpolate_labels

        # Dataframe holding ALL interpolated values
        dtypes = np.dtype([
            ('township', str),
            ('year', int),
            ('interpolated_values', float),
        ])
        data = np.empty(0, dtype=dtypes)
        df = pd.DataFrame(data)

        for (X, y), time_window in self.training_data():
            interpolated_values = interpolate(X=X, X_interpolate_locations=X_interpolate_locations, y=y)

            df_time_window = pd.DataFrame({
                'township': X_interpolate_labels,
                'year': time_window.year,
                'interpolated_values': interpolated_values
            })

            df = df.append(df_time_window)

        # Save dataframe
        save_dataframe_to_csv(
            path=transform_directory / f'township_interpolated_{FINAL_TRANSFORMATION_ID}.csv',
            dataframe=df)


# BIO1 = Annual Mean Temperature
class BioClim_1(BioClim):

    def aggregate(self, dataframe):
        return dataframe.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ]).mean()

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_avg_temperature = dataframe[['temperature_avg']].values

        return df_avg_temperature[:, 0]


# BIO2 = Mean Diurnal Range (sum(month max temp - month min temp)) / 12
class BioClim_2(BioClim):

    def aggregate(self, dataframe):
        # Set columns of interest
        coordinates_columns = set(['latitude', 'longitude', 'altitude'])
        numeric_columns = set(dataframe.select_dtypes(include=np.number).columns.tolist())
        columns_of_interest = list(numeric_columns - coordinates_columns)

        # Select max values per month for numeric columns and give prefix 'max_'
        max_values = dataframe.groupby([
            pd.Grouper(key='date', freq='M'),
            'station_id'
        ])[columns_of_interest].max()

        max_values = max_values.add_prefix('max_')

        # Select min values per month for numeric columns and give prefix 'max_'
        min_values = dataframe.groupby([
            pd.Grouper(key='date', freq='M'),
            'station_id'
        ])[columns_of_interest].min()

        min_values = min_values.add_prefix('min_')

        # Merge min_max values by date + station ID
        min_max_values = max_values.merge(min_values, left_on=['date', 'station_id'], right_on=['date', 'station_id'])

        # Group by year sum
        min_max_values = min_max_values.reset_index()
        min_max_values_year = min_max_values.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ]).sum()

        # Merge longitude and latitude by date + station ID
        # todo refactor
        df_coordinates = dataframe.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ])['longitude', 'latitude'].mean()

        min_max_values_location = min_max_values_year.merge(df_coordinates,
                                                            left_on=['date', 'station_id'],
                                                            right_on=['date', 'station_id'])

        return min_max_values_location

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_min_max_temperature = dataframe[['min_temperature_min', 'max_temperature_max']]

        # Calculate diurmal range
        df_diurmal_range = df_min_max_temperature['max_temperature_max'] - df_min_max_temperature['min_temperature_min']
        df_diurmal_range = df_diurmal_range / 12

        return df_diurmal_range.values


# BIO5 = Max Temperature of Warmest Month
class BioClim_5(BioClim):

    def aggregate(self, dataframe):
        return dataframe.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ]).max()

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_max_temperature = dataframe[['temperature_max']]

        return df_max_temperature.values[:, 0]


# BIO6 = Min Temperature of coldest Month
class BioClim_6(BioClim):

    def aggregate(self, dataframe):
        return dataframe.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ]).min()

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_min_temperature = dataframe[['temperature_min']]

        return df_min_temperature.values[:, 0]


# BIO7 = Temperature Annual Range (BIO5-BIO6)
class BioClim_7(BioClim):

    def aggregate(self, dataframe):
        # Set columns of interest
        coordinates_columns = set(['latitude', 'longitude', 'altitude'])
        numeric_columns = set(dataframe.select_dtypes(include=np.number).columns.tolist())
        columns_of_interest = list(numeric_columns - coordinates_columns)

        # Select max values for numeric columns and give prefix 'max_'
        max_values = dataframe.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ])[columns_of_interest].max()

        max_values = max_values.add_prefix('max_')

        # Select min values for numeric columns and give prefix 'min_'
        min_values = dataframe.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ])[columns_of_interest].min()

        min_values = min_values.add_prefix('min_')

        # Merge min_max values by date + station ID
        min_max_values = max_values.merge(min_values,
                                          left_on=['date', 'station_id'],
                                          right_on=['date', 'station_id'])

        # Merge longitude and latitude by date + station ID
        # todo refactor
        df_coordinates = dataframe.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ])['longitude', 'latitude'].mean()

        min_max_values_location = min_max_values.merge(df_coordinates,
                                                       left_on=['date', 'station_id'],
                                                       right_on=['date', 'station_id'])

        return min_max_values_location

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_min_max_temperature = dataframe[['max_temperature_max', 'min_temperature_min']]

        # Calculate difference
        df_annual_temperature_range = df_min_max_temperature['max_temperature_max'] - df_min_max_temperature[
            'min_temperature_min']

        return df_annual_temperature_range.values


# BIO10 = Mean temperature of warmest quarter
class BioClim_10(BioClim):

    def aggregate(self, dataframe):
        df_quarter = dataframe.groupby([
            pd.Grouper(key='date', freq='Q'),
            'station_id'
        ]).mean()

        df_quarter = df_quarter.reset_index()
        df_year_max_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()

        return df_year_max_quarter

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_avg_temperature = dataframe[['temperature_avg']].values

        return df_avg_temperature[:, 0]


# BIO11 = Mean temperature of coldest quarter
class BioClim_11(BioClim):

    def aggregate(self, dataframe):
        df_quarter = dataframe.groupby([
            pd.Grouper(key='date', freq='Q'),
            'station_id'
        ]).mean()

        df_quarter = df_quarter.reset_index()
        df_year_min_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()

        return df_year_min_quarter

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_avg_temperature = dataframe[['temperature_avg']].values

        return df_avg_temperature[:, 0]


# BIO12 = Annual precipitation
class BioClim_12(BioClim):

    def aggregate(self, dataframe):
        return dataframe.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).sum()

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_rain_sum = dataframe[['rain_sum']].values

        return df_rain_sum[:, 0]


# BIO13 = Precipitation of wettest month
class BioClim_13(BioClim):

    def aggregate(self, dataframe):
        df_month = dataframe.groupby([
            pd.Grouper(key='date', freq='M'),
            'station_id'
        ]).sum()

        df_month = df_month.reset_index()
        df_year_max_month = df_month.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()

        return df_year_max_month

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_rain_sum = dataframe[['rain_sum']].values

        return df_rain_sum[:, 0]


# BIO14 = Precipitation of driest month
class BioClim_14(BioClim):

    def aggregate(self, dataframe):
        df_month = dataframe.groupby([
            pd.Grouper(key='date', freq='M'),
            'station_id'
        ]).sum()

        df_month = df_month.reset_index()
        df_year_min_month = df_month.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()

        return df_year_min_month

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_rain_sum = dataframe[['rain_sum']].values

        return df_rain_sum[:, 0]


# BIO16 = Precipitation of wettest quarter
class BioClim_16(BioClim):

    def aggregate(self, dataframe):
        df_quarter = dataframe.groupby([
            pd.Grouper(key='date', freq='Q'),
            'station_id'
        ]).sum()

        df_quarter = df_quarter.reset_index()
        df_year_max_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()

        return df_year_max_quarter

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_rain_sum = dataframe[['rain_sum']].values

        return df_rain_sum[:, 0]


# BIO17 = Precipitation of driest quarter
class BioClim_17(BioClim):

    def aggregate(self, dataframe):
        df_quarter = dataframe.groupby([
            pd.Grouper(key='date', freq='Q'),
            'station_id'
        ]).sum()

        df_quarter = df_quarter.reset_index()
        df_year_min_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()

        return df_year_min_quarter

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_rain_sum = dataframe[['rain_sum']].values

        return df_rain_sum[:, 0]


# BIO18 = Precipitation of warmest quarter
class BioClim_18(BioClim):

    def aggregate(self, dataframe):
        df_quarter = dataframe.groupby([
            pd.Grouper(key='date', freq='Q'),
            'station_id'
        ]).sum()

        df_quarter = df_quarter.reset_index()
        idx_year_max_sum_temp = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()[
            'temperature_avg'].index
        year_max_sum_temp_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).sum().loc[
            idx_year_max_sum_temp]

        return year_max_sum_temp_quarter

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_rain_sum = dataframe[['rain_sum']].values

        return df_rain_sum[:, 0]


# BIO19 = Precipitation of coldest quarter
class BioClim_19(BioClim):

    def aggregate(self, dataframe):
        df_quarter = dataframe.groupby([
            pd.Grouper(key='date', freq='Q'),
            'station_id'
        ]).sum()

        df_quarter = df_quarter.reset_index()
        idx_year_min_sum_temp = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()[
            'temperature_avg'].index
        year_min_sum_temp_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).sum().loc[
            idx_year_min_sum_temp]

        return year_min_sum_temp_quarter

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_rain_sum = dataframe[['rain_sum']].values

        return df_rain_sum[:, 0]

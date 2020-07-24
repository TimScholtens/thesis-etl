import numpy as np
import geopandas as gpd
import pandas as pd
import shapely.wkt
from etl.transform.transformers.base import Base
from pathlib import Path
from sklearn.neighbors import KNeighborsRegressor
from abc import ABC, abstractmethod
from enum import Enum
from config import FINAL_TRANSFORMATION_ID


def save_dataframe_to_csv(path, dataframe):
    # Create local directory if not exists
    if not path.parent.is_dir():
        Path.mkdir(path.parent, parents=True, exist_ok=True)

    dataframe.to_csv(path, index=False)


def get_weather_station_values(extract_directory):
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


def get_weather_station_coordinates(extract_directory):
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

    df_weather_station_coordinates = pd.read_csv(
        extract_directory / 'station_locations.csv',
        dtype=dtypes,
        usecols=list(column_mapping),
        header=0
    )

    # Rename to more meaningful names
    df_weather_station_coordinates = df_weather_station_coordinates.rename(columns=column_mapping)

    return df_weather_station_coordinates


def get_neighbourhood_data(extract_directory):
    dtypes = {
        "geometry": "str",
        "name": "str",
        "township": "str",
        "centroid": "str",
        "id": "str"
    }

    file_path = extract_directory / 'neighbourhoods.csv'

    df = pd.read_csv(
        file_path,
        dtype=dtypes,
        header=0
    )

    # Transform columns 'geometry','centroid' to data type geometry
    df['geometry'] = df['geometry'].apply(shapely.wkt.loads)
    df['centroid'] = df['centroid'].apply(shapely.wkt.loads)

    # Load into geodataframe
    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    return gdf


def get_training_dataframe(extract_directory):
    # Merge weather station data and their locations
    training_values = get_weather_station_values(extract_directory)
    training_values_coordinates = get_weather_station_coordinates(extract_directory)

    training_dataframe = training_values.merge(training_values_coordinates,
                                               left_on='station_id',
                                               right_on='station_id')

    return training_dataframe


def get_interpolation_coordinates(extract_directory):
    neighbourhood_data = get_neighbourhood_data(extract_directory)

    # Calculate longitudes, latitudes for given centroid points
    neighbourhood_data['centroid_longitude'] = neighbourhood_data['centroid'].apply(lambda point: point.x)
    neighbourhood_data['centroid_latitude'] = neighbourhood_data['centroid'].apply(lambda point: point.y)

    interpolation_coordinates = neighbourhood_data[['centroid_longitude', 'centroid_latitude']].values
    neighbourhood_labels = neighbourhood_data['name'].values
    neighbourhood_ids = neighbourhood_data['id'].values
    township_labels = neighbourhood_data['township'].values

    return interpolation_coordinates, neighbourhood_labels, neighbourhood_ids, township_labels


def interpolate(training_coordinates, training_values, interpolate_coordinates):
    """
    training_coordinates: The coordinates of the known points.
    training_values: The values belonging to the 'training_coordinates'.
    interpolate_coordinates: The coordinates which need to be interpolated.

    returns: numpy array holding the interpolated values for the given 'interpolation_coordinates'
    """
    knn_regressor = KNeighborsRegressor(metric='haversine', algorithm='ball_tree', weights='distance', leaf_size=2)
    knn_regressor.fit(training_coordinates, training_values)

    return knn_regressor.predict(interpolate_coordinates)


class BioClim(Base, ABC):

    def __init__(self, time_partition_strategy):
        self.time_partition_strategy = time_partition_strategy

    def get_base_bioclim_dataframe(self):
        """
        Base dataframe which will hold the interpolated values.
        :return:
        """
        dtypes = np.dtype([
            ('id', str),
            ('name', str),
            ('township', str),
            ('year', int),
            ('interpolated_values', float),
        ])
        data = np.empty(0, dtype=dtypes)
        df = pd.DataFrame(data)

        return df

    def transform(self, extract_directory, transform_directory):
        # Training data
        training_data = get_training_dataframe(extract_directory)

        # Coordinates which have to be interpolated
        interpolate_coordinates, neighbourhood_labels, neighbourhood_ids, township_labels = get_interpolation_coordinates(
            extract_directory=extract_directory)

        # Empty dataframe which will hold the interpolated values
        df = self.get_base_bioclim_dataframe()

        for training_coordinates, training_values, year in self.time_partition_strategy.partition(
                training_data=training_data):
            interpolated_values = interpolate(
                training_coordinates=training_coordinates,
                training_values=training_values,
                interpolate_coordinates=interpolate_coordinates)

            df_time_partition = pd.DataFrame({
                'id': neighbourhood_ids,
                'name': neighbourhood_labels,
                'township': township_labels,
                'year': year,
                'interpolated_values': interpolated_values
            })

            df = df.append(df_time_partition)

        # Save dataframe
        save_dataframe_to_csv(
            path=transform_directory / f'neighbourhood_interpolated_{FINAL_TRANSFORMATION_ID}.csv',
            dataframe=df)


class BioClimEnums(Enum):
    bioclim_1 = 'bioclim_1'
    bioclim_2 = 'bioclim_2'
    bioclim_3 = 'bioclim_3'
    bioclim_4 = 'bioclim_4'
    bioclim_5 = 'bioclim_5'
    bioclim_6 = 'bioclim_6'
    bioclim_7 = 'bioclim_7'
    bioclim_8 = 'bioclim_8'
    bioclim_9 = 'bioclim_9'
    bioclim_10 = 'bioclim_10'
    bioclim_11 = 'bioclim_11'
    bioclim_12 = 'bioclim_12'
    bioclim_13 = 'bioclim_13'
    bioclim_14 = 'bioclim_14'
    bioclim_15 = 'bioclim_15'
    bioclim_16 = 'bioclim_16'
    bioclim_17 = 'bioclim_17'
    bioclim_18 = 'bioclim_18'
    bioclim_19 = 'bioclim_19'


class BioClimFactory:

    @staticmethod
    def get_bioclim(bioclim_id):
        if bioclim_id is BioClimEnums.bioclim_1:
            return BioClim(time_partition_strategy=BioClim1TimePartitionStrategy())
        if bioclim_id is BioClimEnums.bioclim_2:
            return BioClim(time_partition_strategy=BioClim2TimePartitionStrategy())


class BioClimTimePartitionTimeStrategy(ABC):

    @abstractmethod
    def partition(self, training_data):
        pass

    @abstractmethod
    def aggregate(self, training_data):
        pass

    def filter_nan_indexes_training_data(self, training_values, training_coordinates):
        # Remove NaN values
        non_nan_indexes = np.where(~np.isnan(training_values))[0]
        training_coordinates = training_coordinates[non_nan_indexes]
        training_values = training_values[non_nan_indexes]

        return training_coordinates, training_values


# BIO1 = Annual Mean Temperature
class BioClim1TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Aggregate data according to 'BioClim 1' specifications:
            - Mean of average temperature

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'BioClim 1' specification.
        """
        return training_data.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).mean()

    def partition(self, training_data):
        """
            :return: generator with mean temperature for all known points, each yield equals one year.
        """
        aggregated_training_data = self.aggregate(training_data)
        years = set([index[0] for index in aggregated_training_data.index])

        for year in years:
            # Training data frame for current year
            df_year = aggregated_training_data.loc[(year,)]

            # Only select relevant data
            training_coordinates = df_year[['longitude', 'latitude']].values
            training_values = df_year['temperature_avg'].values

            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO2 = Mean Diurnal Range (sum(month max temp - month min temp)) / 12
class BioClim2TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Aggregate data according to 'BioClim 2' specifications:
          - Get the difference between monthly max and min temperature
          - Get the average of these differences over 12 months

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_2' specification.
        """

        # Per month calculate minimal temperature and maximal temperature
        df_monthly_min_max = training_data.groupby([pd.Grouper(key='date', freq='M'), 'station_id']) \
            .agg({'temperature_min': 'min', 'temperature_max': 'max', 'longitude': 'mean', 'latitude': 'mean'})

        # Use 'reset_index' function such that we again can group by indexes 'date' and 'station_id'
        df_monthly_min_max = df_monthly_min_max.reset_index()

        # Get the average of the maximal and minimal temperature over 12 months.
        df_year_temp_mean = df_monthly_min_max.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ]).mean()

        # Calculate the difference between the maximal and minimal temperature, also called the 'diurmal range'.
        df_year_temp_mean['temperature_range'] = df_year_temp_mean['temperature_max'] - df_year_temp_mean[
            'temperature_min']

        return df_year_temp_mean

    def partition(self, training_data):
        """
            :return: generator with mean temperature for all known points, each yield equals one year.
        """
        aggregated_training_data = self.aggregate(training_data)
        years = set([index[0] for index in aggregated_training_data.index])

        for year in years:
            # Training data frame for current year
            df_year = aggregated_training_data.loc[(year,)]

            # Only select relevant data
            training_coordinates = df_year[['longitude', 'latitude']].values
            training_values = df_year['temperature_range'].values

            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO4 = Temperature Seasonality (standard deviation ×100)
class BioClim4TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition: The amount of temperature variation over
        a given year (or averaged years) based on the standard
        deviation (variation) of monthly temperature averages.


        Aggregate data according to 'BioClim 4' specifications:
          - Get monthly average temperature
          - Get the standard deviation of these averages over 12 months

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_4' specification.
        """

        # Per month calculate minimal temperature and maximal temperature
        df_monthly_min_max = training_data.groupby([pd.Grouper(key='date', freq='M'), 'station_id']) \
            .agg({'temperature_min': 'min', 'temperature_max': 'max', 'longitude': 'mean', 'latitude': 'mean'})

        # Use 'reset_index' function such that we again can group by indexes 'date' and 'station_id'
        df_monthly_min_max = df_monthly_min_max.reset_index()

        # Get the average of the maximal and minimal temperature over 12 months.
        df_year_temp_mean = df_monthly_min_max.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ]).mean()

        # Calculate the difference between the maximal and minimal temperature, also called the 'diurmal range'.
        df_year_temp_mean['temperature_range'] = df_year_temp_mean['temperature_max'] - df_year_temp_mean[
            'temperature_min']

        return df_year_temp_mean

    def partition(self, training_data):
        """
            :return: generator with mean temperature for all known points, each yield equals one year.
        """
        aggregated_training_data = self.aggregate(training_data)
        years = set([index[0] for index in aggregated_training_data.index])

        for year in years:
            # Training data frame for current year
            df_year = aggregated_training_data.loc[(year,)]

            # Only select relevant data
            training_coordinates = df_year[['longitude', 'latitude']].values
            training_values = df_year['temperature_range'].values

            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year

# BIO3 = Isothermality (BIO2/ BIO7)(= day-to-night temp. range / winter-summer temp. range)
# class BioClim_3(BioClim):
#
#     def aggregate(self, dataframe):
#         bio2 = BioClim_2().aggregate(dataframe)
#         bio7 = BioClim_7().aggregate(dataframe)
#
#         Merge BIO2, BIO7 based on station_id and year
# df_merge = bio2.merge(bio7, how='left', left_index=True, right_index=True, suffixes=('_BIO_2', '_BIO_7'))
#
# Divide BIO2's day-to-night temperature_range by BIO7's winter-summer temperature_range
# df_merge['isothermality'] = df_merge['temperature_range_BIO_2'].div(df_merge['temperature_range_BIO_7'])
# df_merge['latitude'] = df_merge['latitude_BIO_2']
# df_merge['longitude'] = df_merge['longitude_BIO_2']
#
# return df_merge
#
# def y(self, dataframe):
#     Filter out irrelevant columns
# df_temperature_range = dataframe['isothermality']
#
# return df_temperature_range.values
#
#
# BIO4 = Temperature Seasonality (standard deviation ×100)
# class BioClim_4(BioClim):
#
#     def aggregate(self, dataframe):
#         df_mean_month = dataframe.groupby([
#             pd.Grouper(key='date', freq='M'),
#             'station_id'
#         ]).mean()
#
#         df_mean_month = df_mean_month.reset_index()
#
#         df_std_year = df_mean_month.groupby([
#             pd.Grouper(key='date', freq='Y'),
#             'station_id'
#         ]).std() * 100
#
#         return df_std_year
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_avg_temperature = dataframe['temperature_avg']
#
# return df_avg_temperature.values
#
#
# BIO5 = Max Temperature of Warmest Month
# class BioClim_5(BioClim):
#
#     def aggregate(self, dataframe):
#         return dataframe.groupby([
#             pd.Grouper(key='date', freq='Y'),
#             'station_id'
#         ]).max()
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_max_temperature = dataframe['temperature_max']
#
# return df_max_temperature.values
#
#
# BIO6 = Min Temperature of coldest Month
# class BioClim_6(BioClim):
#
#     def aggregate(self, dataframe):
#         return dataframe.groupby([
#             pd.Grouper(key='date', freq='Y'),
#             'station_id'
#         ]).min()
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_min_temperature = dataframe['temperature_min']
#
# return df_min_temperature.values
#
#
# BIO7 = Temperature Annual Range (BIO5-BIO6)
# class BioClim_7(BioClim):
#
#     def aggregate(self, dataframe):
#         df_year_temp = dataframe.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).agg(
#             {'temperature_min': 'min',
#              'temperature_max': 'max',
#              'longitude': 'mean',
#              'latitude': 'mean'}
#         )
#
#         df_year_temp['temperature_range'] = df_year_temp['temperature_max'] - df_year_temp['temperature_min']
#
#         return df_year_temp
#
#     def y(self, dataframe):
#         return dataframe['temperature_range'].values
#
#
# BIO8 = Mean temperature of wettest quarter
# class BioClim_8(BioClim):
#
#     def aggregate(self, dataframe):
#         df_quarter = dataframe.groupby([
#             pd.Grouper(key='date', freq='Q'),
#             'station_id'
#         ]).sum()
#
#         df_quarter = df_quarter.reset_index()
#         idx_year_max_sum_rain = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()[
#             'rain_sum'].index
#         year_max_sum_rain_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).sum().loc[
#             idx_year_max_sum_rain]
#
#         return year_max_sum_rain_quarter
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_rain_sum = dataframe['temperature_avg']
#
# return df_rain_sum.values
#
#
# BIO9 = Mean temperature of driest quarter
# class BioClim_9(BioClim):
#
#     def aggregate(self, dataframe):
#         df_quarter = dataframe.groupby([
#             pd.Grouper(key='date', freq='Q'),
#             'station_id'
#         ]).sum()
#
#         df_quarter = df_quarter.reset_index()
#         idx_year_min_sum_rain = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()[
#             'rain_sum'].index
#         year_min_sum_rain_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).sum().loc[
#             idx_year_min_sum_rain]
#
#         return year_min_sum_rain_quarter
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_rain_sum = dataframe['temperature_avg']
#
# return df_rain_sum.values
#
#
# BIO10 = Mean temperature of warmest quarter
# class BioClim_10(BioClim):
#
#     def aggregate(self, dataframe):
#         df_quarter = dataframe.groupby([
#             pd.Grouper(key='date', freq='Q'),
#             'station_id'
#         ]).mean()
#
#         df_quarter = df_quarter.reset_index()
#         df_year_max_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()
#
#         return df_year_max_quarter
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_avg_temperature = dataframe['temperature_avg']
#
# return df_avg_temperature.values
#
#
# BIO11 = Mean temperature of coldest quarter
# class BioClim_11(BioClim):
#
#     def aggregate(self, dataframe):
#         df_quarter = dataframe.groupby([
#             pd.Grouper(key='date', freq='Q'),
#             'station_id'
#         ]).mean()
#
#         df_quarter = df_quarter.reset_index()
#         df_year_min_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()
#
#         return df_year_min_quarter
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_avg_temperature = dataframe['temperature_avg']
#
# return df_avg_temperature.values
#
#
# BIO12 = Annual precipitation
# class BioClim_12(BioClim):
#
#     def aggregate(self, dataframe):
#         return dataframe.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).sum()
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_rain_sum = dataframe['rain_sum']
#
# return df_rain_sum.values
#
#
# BIO13 = Precipitation of wettest month
# class BioClim_13(BioClim):
#
#     def aggregate(self, dataframe):
#         df_month = dataframe.groupby([
#             pd.Grouper(key='date', freq='M'),
#             'station_id'
#         ]).sum()
#
#         df_month = df_month.reset_index()
#         df_year_max_month = df_month.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()
#
#         return df_year_max_month
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_rain_sum = dataframe['rain_sum']
#
# return df_rain_sum.values
#
#
# BIO14 = Precipitation of driest month
# class BioClim_14(BioClim):
#
#     def aggregate(self, dataframe):
#         df_month = dataframe.groupby([
#             pd.Grouper(key='date', freq='M'),
#             'station_id'
#         ]).sum()
#
#         df_month = df_month.reset_index()
#         df_year_min_month = df_month.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()
#
#         return df_year_min_month
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_rain_sum = dataframe['rain_sum']
#
# return df_rain_sum.values
#
#
# BIO15 = Precipitation seasonality (CV)
# class BioClim_15(BioClim):
#
#     def aggregate(self, dataframe):
#         df_month = dataframe.groupby([
#             pd.Grouper(key='date', freq='M'),
#             'station_id'
#         ]).sum()
#
#         df_month = df_month.reset_index()
#         df_year_std_month = df_month.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).std()
#
#         bio12 = BioClim_12().aggregate(dataframe)
#         bio12 = bio12.div(12)
#         bio12 = bio12.add(1)
#
#         df_merge = df_year_std_month.merge(bio12, how='left', left_index=True, right_index=True,
#                                            suffixes=('', '_BIO_12'))
#
#         df_merge['precipitation_seasonality'] = df_merge['rain_sum'].div(df_merge['rain_sum_BIO_12'])
#         df_merge['precipitation_seasonality'] = df_merge['precipitation_seasonality'] * 100
#
#         return df_merge
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_precipitation_seasonality = dataframe['precipitation_seasonality']
#
# return df_precipitation_seasonality.values
#
#
# BIO16 = Precipitation of wettest quarter
# class BioClim_16(BioClim):
#
#     def aggregate(self, dataframe):
#         df_quarter = dataframe.groupby([
#             pd.Grouper(key='date', freq='Q'),
#             'station_id'
#         ]).sum()
#
#         df_quarter = df_quarter.reset_index()
#         df_year_max_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()
#
#         return df_year_max_quarter
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_rain_sum = dataframe['rain_sum']
#
# return df_rain_sum.values
#
#
# BIO17 = Precipitation of driest quarter
# class BioClim_17(BioClim):
#
#     def aggregate(self, dataframe):
#         df_quarter = dataframe.groupby([
#             pd.Grouper(key='date', freq='Q'),
#             'station_id'
#         ]).sum()
#
#         df_quarter = df_quarter.reset_index()
#         df_year_min_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()
#
#         return df_year_min_quarter
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_rain_sum = dataframe['rain_sum']
#
# return df_rain_sum.values
#
#
# BIO18 = Precipitation of warmest quarter
# class BioClim_18(BioClim):
#
#     def aggregate(self, dataframe):
#         df_quarter = dataframe.groupby([
#             pd.Grouper(key='date', freq='Q'),
#             'station_id'
#         ]).sum()
#
#         df_quarter = df_quarter.reset_index()
#         idx_year_max_sum_temp = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()[
#             'temperature_avg'].index
#         year_max_sum_temp_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).sum().loc[
#             idx_year_max_sum_temp]
#
#         return year_max_sum_temp_quarter
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_rain_sum = dataframe['rain_sum']
#
# return df_rain_sum.values
#
#
# BIO19 = Precipitation of coldest quarter
# class BioClim_19(BioClim):
#
#     def aggregate(self, dataframe):
#         df_quarter = dataframe.groupby([
#             pd.Grouper(key='date', freq='Q'),
#             'station_id'
#         ]).sum()
#
#         df_quarter = df_quarter.reset_index()
#         idx_year_min_sum_temp = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()[
#             'temperature_avg'].index
#         year_min_sum_temp_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).sum().loc[
#             idx_year_min_sum_temp]
#
#         return year_min_sum_temp_quarter
#
#     def y(self, dataframe):
#         Filter out irrelevant columns
# df_rain_sum = dataframe['rain_sum']
#
# return df_rain_sum.values

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

        # As we only want to interpolate over the spatial dimension, only use data of 1 time unit (year) at a time.
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
        elif bioclim_id is BioClimEnums.bioclim_2:
            return BioClim(time_partition_strategy=BioClim2TimePartitionStrategy())
        elif bioclim_id is BioClimEnums.bioclim_3:
            return BioClim(time_partition_strategy=BioClim3TimePartitionStrategy())
        elif bioclim_id is BioClimEnums.bioclim_4:
            return BioClim(time_partition_strategy=BioClim4TimePartitionStrategy())
        elif bioclim_id is BioClimEnums.bioclim_5:
            return BioClim(time_partition_strategy=BioClim5TimePartitionStrategy())
        elif bioclim_id is BioClimEnums.bioclim_6:
            return BioClim(time_partition_strategy=BioClim6TimePartitionStrategy())
        elif bioclim_id is BioClimEnums.bioclim_7:
            return BioClim(time_partition_strategy=BioClim7TimePartitionStrategy())
        elif bioclim_id is BioClimEnums.bioclim_8:
            return BioClim(time_partition_strategy=BioClim8TimePartitionStrategy())
        elif bioclim_id is BioClimEnums.bioclim_9:
            return BioClim(time_partition_strategy=BioClim9TimePartitionStrategy())
        elif bioclim_id is BioClimEnums.bioclim_10:
            return BioClim(time_partition_strategy=BioClim10TimePartitionStrategy())
        elif bioclim_id is BioClimEnums.bioclim_11:
            return BioClim(time_partition_strategy=BioClim11TimePartitionStrategy())
        elif bioclim_id is BioClimEnums.bioclim_12:
            return BioClim(time_partition_strategy=BioClim12TimePartitionStrategy())
        elif bioclim_id is BioClimEnums.bioclim_13:
            return BioClim(time_partition_strategy=BioClim13TimePartitionStrategy())
        elif bioclim_id is BioClimEnums.bioclim_14:
            return BioClim(time_partition_strategy=BioClim14TimePartitionStrategy())
        elif bioclim_id is BioClimEnums.bioclim_15:
            return BioClim(time_partition_strategy=BioClim15TimePartitionStrategy())
        else:
            raise NotImplementedError


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
        Definition: The annual mean temperature

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

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO2 = Mean Diurnal Range
class BioClim2TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition: The mean of the monthly temperature
        ranges (monthly maximum minus monthly minimum).

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
        df_year_temp_mean = df_monthly_min_max.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).mean()

        # Calculate the difference between the maximal and minimal temperature, also called the 'diurmal range'.
        df_year_temp_mean['temperature_range'] = df_year_temp_mean['temperature_max'] - df_year_temp_mean['temperature_min']

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

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO3 = Isothermality
class BioClim3TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition:  Isothermality quantifies how large the dayto-night temperatures oscillate
        relative to the summerto-winter (annual) oscillations.

        Aggregate data according to 'BioClim 3' specifications:
          - (BIOCLIM 2 / BIOCLIM 7) * 100

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_3' specification.
        """
        df_year_month_range = BioClim2TimePartitionStrategy().aggregate(training_data)
        df_year_min_max_range = BioClim7TimePartitionStrategy().aggregate(training_data)

        # Inner join both dataframes based on indexes (date, station_id)
        df_year_iso = df_year_month_range.merge(df_year_month_range,
                                                how='left',
                                                left_index=True,
                                                right_index=True,
                                                suffixes=('_BIO_2', '_BIO_7'))

        # Divide BIO2's day-to-night temperature_range by BIO7's winter-summer temperature_range
        df_year_iso['isothermality'] = df_year_iso['temperature_range_BIO_2'].div(
            df_year_iso['temperature_range_BIO_7']) * 100

        return df_year_iso

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
            training_coordinates = df_year[['longitude_BIO_2', 'latitude_BIO_2']].values
            training_values = df_year['isothermality'].values

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO4 = Temperature Seasonality
class BioClim4TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition: The amount of temperature variation over
        a given year (or averaged years) based on the standard
        deviation (variation) of monthly temperature averages.


        Aggregate data according to 'BioClim 4' specifications:
          - Get monthly average temperature.
          - Get the standard deviation of these averages over 12 months, and multiply by 100.

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_4' specification.
        """
        # Calculate average values per month
        df_monthly_mean = training_data.groupby([pd.Grouper(key='date', freq='M'), 'station_id']).mean()

        # Use 'reset_index' function such that we again can group by indexes 'date' and 'station_id'
        df_monthly_mean = df_monthly_mean.reset_index()

        # Calculate standard deviation over 12 months and multiply by 100
        df_year_std = df_monthly_mean.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).std() * 100

        return df_year_std

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

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO5 = Maximum temperature of warmest month
class BioClim5TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition: The maximum monthly temperature occurrence over a given year (time-series) or averaged span
        of years (normal)

        Aggregate data according to 'BioClim 5' specifications:
          - Get monthly average values.
          - Get the max value of these averages within 12 months.

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_5' specification.
        """
        # Calculate average values per month
        df_monthly_mean = training_data.groupby([pd.Grouper(key='date', freq='M'), 'station_id']).mean()

        # Use 'reset_index' function such that we again can group by indexes 'date' and 'station_id'
        df_monthly_mean = df_monthly_mean.reset_index()

        # Calculate max temperature within 12 months
        df_year_max = df_monthly_mean.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()

        return df_year_max

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
            training_values = df_year['temperature_max'].values

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO6 = Minimum temperature of coldest month
class BioClim6TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition: The minimum monthly temperature occurrence over a given year (time-series) or averaged span
        of years (normal)

        Aggregate data according to 'BioClim 6' specifications:
          - Get monthly average values.
          - Get the min value of these averages within 12 months.

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_6' specification.
        """
        # Calculate average values per month
        df_monthly_mean = training_data.groupby([pd.Grouper(key='date', freq='M'), 'station_id']).mean()

        # Use 'reset_index' function such that we again can group by indexes 'date' and 'station_id'
        df_monthly_mean = df_monthly_mean.reset_index()

        # Calculate min temperature within 12 months
        df_year_min = df_monthly_mean.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()

        return df_year_min

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
            training_values = df_year['temperature_min'].values

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO7 = Annual temperature range
class BioClim7TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition:  A measure of temperature variation over a given period.

        Aggregate data according to 'BioClim 7' specifications:
          - BIOCLIM 5(year_max_temp) - BIOCLIM 6(year_min_temp)

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_7' specification.
        """
        df_year_max = BioClim5TimePartitionStrategy().aggregate(training_data)
        df_year_min = BioClim6TimePartitionStrategy().aggregate(training_data)

        # Inner join both dataframes based on indexes (date, station_id)
        df_year_diff = df_year_max.merge(df_year_min, how='left', left_index=True, right_index=True,
                                         suffixes=('_BIO_5', '_BIO_6'))

        # Calculate temperature difference
        df_year_diff['temperature_range'] = df_year_diff['temperature_max_BIO_5'] - df_year_diff[
            'temperature_min_BIO_6']

        return df_year_diff

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
            training_coordinates = df_year[['longitude_BIO_5', 'latitude_BIO_5']].values
            training_values = df_year['temperature_range'].values

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO8 = Mean temperature of wettest quarter
class BioClim8TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition:  This quarterly index approximates mean
        temperatures that prevail during the wettest season.

        Aggregate data according to 'BioClim 8' specifications:
            - Get quarterly sum values.
            - Select quarter which has the highest sum of precipitation.
                - Divide the sum of average temperature by 3.

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_8' specification.
        """
        # Calculate quarterly sums
        df_quarter = training_data.groupby([pd.Grouper(key='date', freq='Q'), 'station_id']).sum()

        # Use 'reset_index' function such that we again can group by indexes 'date' and 'station_id'
        df_quarter = df_quarter.reset_index()

        # Get indexes of most wettest quarters (sum)
        df_quarter_max_rain_index = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()['rain_sum'].index

        # Use above indexes to get the related mean temperature
        df_year_avg_temp = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).mean().loc[df_quarter_max_rain_index]

        return df_year_avg_temp

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

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO9 = Mean temperature of driest quarter
class BioClim9TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition:  This quarterly index approximates mean
        temperatures that prevail during the driest season.

        Aggregate data according to 'BioClim 9' specifications:
            - Get quarterly sum values.
            - Select quarter which has the lowest sum of precipitation.
                - Divide the sum of average temperature by 3.

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_9' specification.
        """
        # Calculate quarterly sums
        df_quarter = training_data.groupby([pd.Grouper(key='date', freq='Q'), 'station_id']).sum()

        # Use 'reset_index' function such that we again can group by indexes 'date' and 'station_id'
        df_quarter = df_quarter.reset_index()

        # Get indexes of most driest quarters (sum)
        df_quarter_min_rain_index = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()['rain_sum'].index

        # Use above indexes to get the related mean temperature
        df_year_avg_temp = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).mean().loc[df_quarter_min_rain_index]

        return df_year_avg_temp

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

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO10 = Mean temperature of warmest quarter
class BioClim10TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition:  This quarterly index approximates mean
        temperatures that prevail during the warmest season.

        Aggregate data according to 'BioClim 10' specifications:
            - Get quarterly mean values.
            - Select quarter which has the highest temperature.

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_10' specification.
        """
        # Calculate quarterly means
        df_quarter = training_data.groupby([pd.Grouper(key='date', freq='Q'), 'station_id']).mean()

        # Use 'reset_index' function such that we again can group by indexes 'date' and 'station_id'
        df_quarter = df_quarter.reset_index()

        # Use above indexes to get the related mean temperature
        df_year_max_avg_temp = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()

        return df_year_max_avg_temp

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

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO11 = Mean temperature of coldest quarter
class BioClim11TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition:  This quarterly index approximates mean
        temperatures that prevail during the coldest season.

        Aggregate data according to 'BioClim 11' specifications:
            - Get quarterly mean values.
            - Select quarter which has the lowest temperature.

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_11' specification.
        """
        # Calculate quarterly means
        df_quarter = training_data.groupby([pd.Grouper(key='date', freq='Q'), 'station_id']).mean()

        # Use 'reset_index' function such that we again can group by indexes 'date' and 'station_id'
        df_quarter = df_quarter.reset_index()

        # Use above indexes to get the related mean temperature
        df_year_min_avg_temp = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()

        return df_year_min_avg_temp

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

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO12 = Annual precipitation
class BioClim12TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition:   This is the sum of all total monthly precipitation values.

        Aggregate data according to 'BioClim 12' specifications:
          - Sum of all rain_sum values

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_12' specification.
        """
        return training_data.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).sum()

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
            training_values = df_year['rain_sum'].values

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO13 = Precipitation of wettest month
class BioClim13TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition:   This index identifies the total precipitation that prevails during the wettest month.

        Aggregate data according to 'BioClim 13' specifications:
          - Sum of monthly rain_sum values
          - Select month with the highest rain_sum

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_13' specification.
        """
        # Calculate sum values per month
        df_month_sum = training_data.groupby([pd.Grouper(key='date', freq='M'), 'station_id']).sum()

        # Use 'reset_index' function such that we again can group by indexes 'date' and 'station_id'
        df_month_sum = df_month_sum.reset_index()

        # Calculate month with maximum rain_sum within 12 months
        df_max_month_sum = df_month_sum.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()

        return df_max_month_sum

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
            training_values = df_year['rain_sum'].values

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO14 = Precipitation of driest month
class BioClim14TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition:   This index identifies the total precipitation that prevails during the driest month.

        Aggregate data according to 'BioClim 14' specifications:
          - Sum of monthly rain_sum values
          - Select quarter with the lowest rain_sum

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_14' specification.
        """
        # Calculate sum values per month
        df_month_sum = training_data.groupby([pd.Grouper(key='date', freq='Q'), 'station_id']).sum()

        # Use 'reset_index' function such that we again can group by indexes 'date' and 'station_id'
        df_month_sum = df_month_sum.reset_index()

        # Calculate month with minimum rain_sum within 12 months
        df_min_month_sum = df_month_sum.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()

        return df_min_month_sum

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
            training_values = df_year['rain_sum'].values

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year


# BIO15 = Precipitation seasonality
class BioClim15TimePartitionStrategy(BioClimTimePartitionTimeStrategy):

    def aggregate(self, training_data):
        """
        Definition: The amount of precipitation variation over
        a given year (or averaged years) based on the standard
        deviation (variation) of monthly total precipitation.


        Aggregate data according to 'BioClim 15' specifications:
          - Get monthly total and mean precipitation.
          - Get the standard deviation of these totals over 12 months,
            divide this by 1 + the monthly mean, and finally multiply by 100.

        More details can be found in the link provided in the 'README.MD' file.

        :param training_data: data which needs to be aggregated,
        :return: aggregated dataframe, by the 'bioclim_15' specification.
        """

        # Calculate monthly sum and mean for 'rain_sum'
        df_month_sum_avg = training_data.groupby([pd.Grouper(key='date', freq='M'), 'station_id']) \
            .agg(rain_sum_total=('rain_sum', 'sum'),
                 rain_sum_mean=('rain_sum', 'mean'),
                 longitude=('longitude', 'mean'),
                 latitude=('latitude', 'mean'))

        # Use 'reset_index' function such that we again can group by indexes 'date' and 'station_id'
        df_month_sum_avg = df_month_sum_avg.reset_index()

        # Calculate standard deviation and mean for 'rain_sum' over 12 months
        df_year_std_mean = df_month_sum_avg.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']) \
            .agg(rain_sum_mean=('rain_sum_total', 'mean'),
                 rain_sum_std=('rain_sum_mean', 'std'),
                 longitude=('longitude', 'mean'),
                 latitude=('latitude', 'mean'))

        # Calculate BIOCLIM 15
        df_year_std_mean['BIOCLIM_15'] = df_year_std_mean['rain_sum_std'].div(1 + df_year_std_mean['rain_sum_mean']) * 100

        return df_year_std_mean

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
            training_values = df_year['BIOCLIM_15'].values

            # Filter out NaN values
            training_coordinates, training_values = self.filter_nan_indexes_training_data(
                training_coordinates=training_coordinates,
                training_values=training_values)

            yield training_coordinates, training_values, year

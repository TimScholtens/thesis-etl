import numpy as np
import geopandas as gpd
import pandas as pd
import shapely.wkt
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


def get_neighbourhood_coordinates(extract_directory):
    dtypes = {
        "geometry": "str",
        "name": "str",
        "township": "str",
        "centroid": "str"
    }

    file_path = extract_directory / 'neighbourhoods.csv'

    df = pd.read_csv(
        file_path,
        dtype=dtypes,
        header=0
    )

    # Load into geodataframe
    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    # Calculate center of polygon
    gdf['centroid'] = gdf['centroid'].apply(shapely.wkt.loads)

    return gdf


def get_training_data(extract_directory):
    # Merge weather station data and their locations
    training_values = get_weather_station_values(extract_directory)
    training_values_coordinates = get_weather_station_coordinates(extract_directory)

    training_data = training_values.merge(training_values_coordinates,
                                          left_on='station_id',
                                          right_on='station_id')

    return training_data


def get_partitioned_training_data(self):
    """
        Returns a generator which partitions the data supplied by the 'partition' function.
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


def get_interpolation_coordinates(extract_directory):
    return load_neighbourhood_data(extract_directory)


def interpolate(X, X_interpolate_locations, y):
    """
    X: training coordinates
    X_interpolate: points which have to be interpolated
    y: y training values

    returns: numpy array holding all interpolated values
    """
    knn_regressor = KNeighborsRegressor(metric='haversine', algorithm='ball_tree', weights='distance', leaf_size=2)
    knn_regressor.fit(X, y)

    return knn_regressor.predict(X_interpolate_locations)


class BioClim(Base, ABC):

    def __init__(self, partition_strategy):
        self.partition_strategy = partition_strategy

    def transform(self, extract_directory, transform_directory):
        # Locations which have to be interpolated
        interpolation_coordinates = get_interpolation_coordinates(extract_directory=extract_directory)

        # Dataframe holding ALL interpolated values
        dtypes = np.dtype([
            ('township', str),
            ('year', int),
            ('interpolated_values', float),
        ])
        data = np.empty(0, dtype=dtypes)
        df = pd.DataFrame(data)

        for (X, y), time_window in get_partitioned_training_data():
            interpolated_values = interpolate(X=X, X_interpolate_locations=interpolation_coordinates, y=y)

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


class BioClimPartitionStrategy(ABC):

    @abstractmethod
    def partition(self):
        pass


# BIO1 = Annual Mean Temperature
class BioClim1(BioClimPartitionStrategy):

    def partition(self):
        pass

    def aggregate(self, dataframe):
        return dataframe.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ]).mean()

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_avg_temperature = dataframe['temperature_avg']

        return df_avg_temperature.values


# BIO2 = Mean Diurnal Range (sum(month max temp - month min temp)) / 12
class BioClim_2(BioClim):

    def aggregate(self, dataframe):
        # Per month calculate min temperature_min and max temperature_max
        df_monthly_min_max = dataframe.groupby([
            pd.Grouper(key='date', freq='M'),
            'station_id'
        ]).agg({'temperature_min': 'min', 'temperature_max': 'max', 'longitude': 'mean', 'latitude': 'mean'})

        df_monthly_min_max = df_monthly_min_max.reset_index()

        df_year_temp_mean = df_monthly_min_max.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ]).mean()

        df_year_temp_mean['temperature_range'] = df_year_temp_mean['temperature_max'] - df_year_temp_mean[
            'temperature_min']

        return df_year_temp_mean

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_temperature_range = dataframe['temperature_range']

        return df_temperature_range.values


# BIO3 = Isothermality (BIO2/ BIO7)(= day-to-night temp. range / winter-summer temp. range)
class BioClim_3(BioClim):

    def aggregate(self, dataframe):
        bio2 = BioClim_2().aggregate(dataframe)
        bio7 = BioClim_7().aggregate(dataframe)

        # Merge BIO2, BIO7 based on station_id and year
        df_merge = bio2.merge(bio7, how='left', left_index=True, right_index=True, suffixes=('_BIO_2', '_BIO_7'))

        # Divide BIO2's day-to-night temperature_range by BIO7's winter-summer temperature_range
        df_merge['isothermality'] = df_merge['temperature_range_BIO_2'].div(df_merge['temperature_range_BIO_7'])
        df_merge['latitude'] = df_merge['latitude_BIO_2']
        df_merge['longitude'] = df_merge['longitude_BIO_2']

        return df_merge

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_temperature_range = dataframe['isothermality']

        return df_temperature_range.values


# BIO4 = Temperature Seasonality (standard deviation ×100)
class BioClim_4(BioClim):

    def aggregate(self, dataframe):
        df_mean_month = dataframe.groupby([
            pd.Grouper(key='date', freq='M'),
            'station_id'
        ]).mean()

        df_mean_month = df_mean_month.reset_index()

        df_std_year = df_mean_month.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ]).std() * 100

        return df_std_year

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_avg_temperature = dataframe['temperature_avg']

        return df_avg_temperature.values


# BIO5 = Max Temperature of Warmest Month
class BioClim_5(BioClim):

    def aggregate(self, dataframe):
        return dataframe.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ]).max()

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_max_temperature = dataframe['temperature_max']

        return df_max_temperature.values


# BIO6 = Min Temperature of coldest Month
class BioClim_6(BioClim):

    def aggregate(self, dataframe):
        return dataframe.groupby([
            pd.Grouper(key='date', freq='Y'),
            'station_id'
        ]).min()

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_min_temperature = dataframe['temperature_min']

        return df_min_temperature.values


# BIO7 = Temperature Annual Range (BIO5-BIO6)
class BioClim_7(BioClim):

    def aggregate(self, dataframe):
        df_year_temp = dataframe.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).agg(
            {'temperature_min': 'min',
             'temperature_max': 'max',
             'longitude': 'mean',
             'latitude': 'mean'}
        )

        df_year_temp['temperature_range'] = df_year_temp['temperature_max'] - df_year_temp['temperature_min']

        return df_year_temp

    def y(self, dataframe):
        return dataframe['temperature_range'].values


# BIO8 = Mean temperature of wettest quarter
class BioClim_8(BioClim):

    def aggregate(self, dataframe):
        df_quarter = dataframe.groupby([
            pd.Grouper(key='date', freq='Q'),
            'station_id'
        ]).sum()

        df_quarter = df_quarter.reset_index()
        idx_year_max_sum_rain = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).max()[
            'rain_sum'].index
        year_max_sum_rain_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).sum().loc[
            idx_year_max_sum_rain]

        return year_max_sum_rain_quarter

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_rain_sum = dataframe['temperature_avg']

        return df_rain_sum.values


# BIO9 = Mean temperature of driest quarter
class BioClim_9(BioClim):

    def aggregate(self, dataframe):
        df_quarter = dataframe.groupby([
            pd.Grouper(key='date', freq='Q'),
            'station_id'
        ]).sum()

        df_quarter = df_quarter.reset_index()
        idx_year_min_sum_rain = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).min()[
            'rain_sum'].index
        year_min_sum_rain_quarter = df_quarter.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).sum().loc[
            idx_year_min_sum_rain]

        return year_min_sum_rain_quarter

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_rain_sum = dataframe['temperature_avg']

        return df_rain_sum.values


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
        df_avg_temperature = dataframe['temperature_avg']

        return df_avg_temperature.values


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
        df_avg_temperature = dataframe['temperature_avg']

        return df_avg_temperature.values


# BIO12 = Annual precipitation
class BioClim_12(BioClim):

    def aggregate(self, dataframe):
        return dataframe.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).sum()

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_rain_sum = dataframe['rain_sum']

        return df_rain_sum.values


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
        df_rain_sum = dataframe['rain_sum']

        return df_rain_sum.values


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
        df_rain_sum = dataframe['rain_sum']

        return df_rain_sum.values


# BIO15 = Precipitation seasonality (CV)
class BioClim_15(BioClim):

    def aggregate(self, dataframe):
        df_month = dataframe.groupby([
            pd.Grouper(key='date', freq='M'),
            'station_id'
        ]).sum()

        df_month = df_month.reset_index()
        df_year_std_month = df_month.groupby([pd.Grouper(key='date', freq='Y'), 'station_id']).std()

        bio12 = BioClim_12().aggregate(dataframe)
        bio12 = bio12.div(12)
        bio12 = bio12.add(1)

        df_merge = df_year_std_month.merge(bio12, how='left', left_index=True, right_index=True,
                                           suffixes=('', '_BIO_12'))

        df_merge['precipitation_seasonality'] = df_merge['rain_sum'].div(df_merge['rain_sum_BIO_12'])
        df_merge['precipitation_seasonality'] = df_merge['precipitation_seasonality'] * 100

        return df_merge

    def y(self, dataframe):
        # Filter out irrelevant columns
        df_precipitation_seasonality = dataframe['precipitation_seasonality']

        return df_precipitation_seasonality.values


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
        df_rain_sum = dataframe['rain_sum']

        return df_rain_sum.values


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
        df_rain_sum = dataframe['rain_sum']

        return df_rain_sum.values


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
        df_rain_sum = dataframe['rain_sum']

        return df_rain_sum.values


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
        df_rain_sum = dataframe['rain_sum']

        return df_rain_sum.values

from etl.transform.transformers.base import Base
from pathlib import Path
from sklearn.neighbors import BallTree, KNeighborsRegressor
import json
import geopandas as gpd
import pandas as pd


def load_weather_station_locations(extract_directory):
    df_weather_station_location = gpd.read_file(extract_directory / 'station_locations.csv')

    # Rename to more readable names,
    # note: only select columns which are related to BIOCLIM being temperature and perception
    column_mapping = {
        'STN': 'station_id',
        'LON(east)': 'longitude',
        'LAT(north)': 'latitude',
        'ALT(m)': 'altitude',
        'NAME': 'name'
    }

    # Filter columns
    columns_of_interest = [column for column in column_mapping.keys()]
    df_weather_station_location = df_weather_station_location[columns_of_interest]

    # Rename
    df_weather_station_location = df_weather_station_location.rename(columns=column_mapping)

    # Set data types
    df_weather_station_location['station_id'] = pd.to_numeric(df_weather_station_location['station_id'],
                                                              errors='coerce')
    df_weather_station_location['longitude'] = pd.to_numeric(df_weather_station_location['longitude'], errors='coerce')
    df_weather_station_location['latitude'] = pd.to_numeric(df_weather_station_location['latitude'], errors='coerce')
    df_weather_station_location['altitude'] = pd.to_numeric(df_weather_station_location['altitude'], errors='coerce')

    return df_weather_station_location


def load_weather_station_data(extract_directory):
    """
        Documentation for 'station_data.csv'
            # YYYYMMDD = Datum (YYYY=jaar MM=maand DD=dag);
            # DDVEC    = Vectorgemiddelde windrichting in graden (359=noord, 90=oost, 180=zuid, 270=west, 0=windstil/variabel). Zie http://www.knmi.nl/kennis-en-datacentrum/achtergrond/klimatologische-brochures-en-boeken;
            # FHVEC    = Vectorgemiddelde windsnelheid (in -1.1 m/s). Zie http://www.knmi.nl/kennis-en-datacentrum/achtergrond/klimatologische-brochures-en-boeken;
            # FG       = Etmaalgemiddelde windsnelheid (in -1.1 m/s);
            # FHX      = Hoogste uurgemiddelde windsnelheid (in -1.1 m/s);
            # FHXH     = Uurvak waarin FHX is gemeten;
            # FHN      = Laagste uurgemiddelde windsnelheid (in -1.1 m/s);
            # FHNH     = Uurvak waarin FHN is gemeten;
            # FXX      = Hoogste windstoot (in -1.1 m/s);
            # FXXH     = Uurvak waarin FXX is gemeten;
            # TG       = Etmaalgemiddelde temperatuur (in -1.1 graden Celsius);
            # TN       = Minimum temperatuur (in -1.1 graden Celsius);
            # TNH      = Uurvak waarin TN is gemeten;
            # TX       = Maximum temperatuur (in -1.1 graden Celsius);
            # TXH      = Uurvak waarin TX is gemeten;
            # T9N     = Minimum temperatuur op 10 cm hoogte (in 0.1 graden Celsius);
            # T9NH    = 6-uurs tijdvak waarin T10N is gemeten; 6=0-6 UT, 12=6-12 UT, 18=12-18 UT, 24=18-24 UT
            # SQ       = Zonneschijnduur (in -1.1 uur) berekend uit de globale straling (-1 voor <0.05 uur);
            # SP       = Percentage van de langst mogelijke zonneschijnduur;
            # Q        = Globale straling (in J/cm1);
            # DR       = Duur van de neerslag (in -1.1 uur);
            # RH       = Etmaalsom van de neerslag (in -1.1 mm) (-1 voor <0.05 mm);
            # RHX      = Hoogste uursom van de neerslag (in -1.1 mm) (-1 voor <0.05 mm);
            # RHXH     = Uurvak waarin RHX is gemeten;
            # PG       = Etmaalgemiddelde luchtdruk herleid tot zeeniveau (in -1.1 hPa) berekend uit 24 uurwaarden;
            # PX       = Hoogste uurwaarde van de luchtdruk herleid tot zeeniveau (in -1.1 hPa);
            # PXH      = Uurvak waarin PX is gemeten;
            # PN       = Laagste uurwaarde van de luchtdruk herleid tot zeeniveau (in -1.1 hPa);
            # PNH      = Uurvak waarin PN is gemeten;
            # VVN      = Minimum opgetreden zicht; -1: <100 m, 1:100-200 m, 2:200-300 m,..., 49:4900-5000 m, 50:5-6 km, 56:6-7 km, 57:7-8 km,..., 79:29-30 km, 80:30-35 km, 81:35-40 km,..., 89: >70 km)
            # VVNH     = Uurvak waarin VVN is gemeten;
            # VVX      = Maximum opgetreden zicht; -1: <100 m, 1:100-200 m, 2:200-300 m,..., 49:4900-5000 m, 50:5-6 km, 56:6-7 km, 57:7-8 km,..., 79:29-30 km, 80:30-35 km, 81:35-40 km,..., 89: >70 km)
            # VVXH     = Uurvak waarin VVX is gemeten;
            # NG       = Etmaalgemiddelde bewolking (bedekkingsgraad van de bovenlucht in achtsten, 8=bovenlucht onzichtbaar);
            # UG       = Etmaalgemiddelde relatieve vochtigheid (in procenten);
            # UX       = Maximale relatieve vochtigheid (in procenten);
            # UXH      = Uurvak waarin UX is gemeten;
            # UN       = Minimale relatieve vochtigheid (in procenten);
            # UNH      = Uurvak waarin UN is gemeten;
            # EV23     = Referentiegewasverdamping (Makkink) (in 0.1 mm);
    """

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
        def calculate_townships_centroids(df_township):
            # Get centroids
            df_township_centroid = gpd.GeoDataFrame(df_township.centroid, columns=['centroid'], geometry='centroid')

            # Concat
            df_township_centroid = pd.concat([df_township_centroid, df_township[['code', 'name']]], axis=0)

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
            townships_interpolated_temperature = []

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
                def interpolate_temperature(row):
                    latitude = row['']
                    return row

                df_townships_centroids['interpolated_temperature'] = df_townships_centroids['centroid'].apply(interpolate_temperature,axis=1)

                print(ws_yearly_average_temperature)

        # Load data
        weather_station_data = load_weather_station_data(extract_directory)
        weather_station_locations = load_weather_station_locations(extract_directory)
        townships = load_township_data(extract_directory)

        # Calculate centroids for each township
        df_townships_centroids = calculate_townships_centroids(townships)

        # Filter columns for each weather station
        df_ws_avg_temperature = weather_station_data[['temperature_avg', 'station_id', 'date']]

        # Filter out temperature records having no value
        df_ws_avg_temperature = df_ws_avg_temperature.dropna()

        # Calculate yearly average temperature for each weather station, save output
        df_ws_yearly_avg_temperature = yearly_avg_temperature_weather_station_data(df_ws_avg_temperature)

        # Interpolate for each township the annual temperature, save output "FINAL"
        interpolate(ws_yearly_average_temperature=df_ws_yearly_avg_temperature,
                    ws_location=weather_station_locations,
                    df_townships_centroids=df_townships_centroids)


class BioClim_2(Base):
    pass

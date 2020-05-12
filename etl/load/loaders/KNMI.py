import csv
from datetime import datetime
from shapely.geometry import Point
from etl.load.loaders.base import Base
from etl.load.loader import final_transformation_files
from sqlalchemy.orm import sessionmaker

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


def _float_default_value(val: str):
    """
    Will return NaN if input is an empty string.
    :param val: string to parse
    :return: float on non-empty input string, else NaN
    """
    return float(val) if val else float('nan')


class KNMI(Base):

    def load(self, transform_directory):
        # Workaround - Import here to prevent cyclic import
        from etl.load.models.KNMI import WeatherStationLocation as WeatherStationLocationObject
        from etl.load.models.KNMI import WeatherStationData as WeatherStationDataObject
        import config

        for file in final_transformation_files(transform_directory=transform_directory):
            file_path = transform_directory / file

            with open(file_path) as f:

                csv_reader = csv.DictReader(f, delimiter=',')  # quote non to skip whitespace

                if file == 'station_locations.csv':
                    weather_station_locations = [WeatherStationLocationObject(
                        id=row['STN'],
                        name=row['NAME'],
                        geometry=Point(float(row['LON(east)']), float(row['LAT(north)'])).wkt
                    ) for row in csv_reader]

                    session = sessionmaker(bind=config.SQLALCHEMY_ENGINE)()
                    session.bulk_save_objects(weather_station_locations)
                    session.commit()

                if file == 'station_data.csv':
                    weather_station_data = [WeatherStationDataObject(
                        station_id=row['STN'],
                        date=datetime.strptime(row['YYYYMMDD'], "%Y%m%d"),
                        temperature_avg=_float_default_value(row['TG']) * 0.1,
                        temperature_min=_float_default_value(row['TN']) * 0.1,
                        temperature_max=_float_default_value(row['TX']) * 0.1,
                        sunshine_duration=_float_default_value(row['SQ']) * 0.1,
                        sunshine_radiation=_float_default_value(row['Q']),
                        rain_duration=_float_default_value(row['DR']) * 0.1,
                        rain_sum=_float_default_value(row['RH']),
                        humidity_avg=_float_default_value(row['UG']),
                        humidity_max=_float_default_value(row['UX']),
                        humidity_min=_float_default_value(row['UN'])
                    ) for row in csv_reader]

                    session = sessionmaker(bind=config.SQLALCHEMY_ENGINE)()
                    session.bulk_save_objects(weather_station_data)
                    session.commit()

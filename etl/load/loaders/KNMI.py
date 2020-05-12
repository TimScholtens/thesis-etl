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


class KNMI(Base):

    def load(self, transform_directory):
        # Workaround - Import here to prevent cyclic import
        from etl.load.models.KNMI import WeatherStationLocation as WeatherStationLocationObject
        from etl.load.models.KNMI import WeatherStationData as WeatherStationDataObject
        import config

        for file in final_transformation_files(transform_directory=transform_directory):
            file_path = transform_directory / file

            with open(file_path) as f:

                csv_reader = csv.DictReader(f, delimiter=',') # quote non to skip whitespace

                if file == 'station_locations.csv':
                    weather_station_locations = [WeatherStationLocationObject(
                        id=row['STN'],
                        name=row['NAME'],
                        geometry=Point(float(row['LON(east)']), float(row['LAT(north)'])).wkt
                    ) for row in csv_reader]

                    session = sessionmaker(bind=config.SQLALCHEMY_ENGINE)()
                    session.add_all(weather_station_locations)
                    session.commit()

                if file == 'station_data.csv':

                    Try again using geopandas, has support for geometry
                        https: // hackersandslackers.com / connecting - pandas - to - a - sql - database -
                        with-sqlalchemy /
                        https: // stackoverflow.com / questions / 38361336 / write - geodataframe - into - sql - database
                        https: // www.enigma.com / blog / post / scaling - a - pandas - etl - job - to - 600
                        gb


                    weather_station_data = [WeatherStationDataObject(
                        station_id=row['STN'],
                        date=datetime.strptime(row['YYYYMMDD'], "%Y%m%d"),
                        temperature_avg=float(row['TG']) * 0.1,
                        temperature_min=float(row['TN']) * 0.1,
                        temperature_max=float(row['TX']) * 0.1,
                        sunshine_duration=float(row['SQ']) * 0.1,
                        sunshine_radiation=float(row['Q']),
                        rain_duration=float(row['DR']) * 0.1,
                        rain_sum=float(row['RH']),
                        humidity_avg=float(row['UG']),
                        humidity_max=float(row['UX']),
                        humidity_min=float(row['UN'])
                    ) for row in csv_reader]

                    session = sessionmaker(bind=config.SQLALCHEMY_ENGINE)()
                    session.add_all(weather_station_data)
                    session.commit()

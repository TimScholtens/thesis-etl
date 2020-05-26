from etl.transform.transformers.base import Base
import csv
from datetime import datetime
from decimal import Decimal
from pathlib import Path
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


def _decimal_default_value(val: str, divide_by=1):
    """
    Will return None if input is an empty string.
    :param val: string to parse
    :param divide_by: (workaround) if number has to be divided
    :return: float on non-empty input string, else NaN
    """
    return Decimal(val) / divide_by if val else 'nan'


class KNMIWeatherStationData(Base):

    def transform(self, extract_directory, transform_directory):
        import config
        """
        - Only select columns: STN, YYYYMMDD, TG, TN, TX, SQ, Q, DR, RH, UG, UX, UN
        - Rename columns:
            STN -> station_id
            YYYYMMDD -> date
            TG -> temperature_avg
            TN -> temperature_min
            TX -> temperature_max
            SQ -> sunshine_duration
            Q  -> sunshine_radiation
            DR -> rain_duration
            RH -> rain_sum
            UG -> humidity_avg
            UX -> humidity_max
            UN -> humidity_min

        - Impute missing data
        """

        file_path = extract_directory / 'station_data.csv'

        with open(file_path) as input_file:
            csv_reader = csv.DictReader(input_file, delimiter=',')

            weather_station_data = [dict(
                station_id=row['STN'],
                date=row['YYYYMMDD'],
                temperature_avg=_decimal_default_value(row['TG'], 10),
                temperature_min=_decimal_default_value(row['TN'], 10),
                temperature_max=_decimal_default_value(row['TX'], 10),
                sunshine_duration=_decimal_default_value(row['SQ'], 10),
                sunshine_radiation=_decimal_default_value(row['Q'], 10),
                rain_duration=_decimal_default_value(row['DR'], 10),
                rain_sum=_decimal_default_value(row['RH']),
                humidity_avg=_decimal_default_value(row['UG']),
                humidity_max=_decimal_default_value(row['UX']),
                humidity_min=_decimal_default_value(row['UN'])
            ) for row in csv_reader]

        # Set output file
        final_file_name = f'station_data_{config.FINAL_TRANSFORMATION_ID}.csv'
        output_file_path = transform_directory / final_file_name

        # Create local directory if not exists
        if not Path(transform_directory).is_dir():
            Path.mkdir(transform_directory)

        # Write transformations to file
        with open(output_file_path, 'w+') as output_file:
            csv_writer = csv.DictWriter(output_file,
                                        fieldnames=weather_station_data[0].keys(),
                                        delimiter=',',
                                        lineterminator='\n')

            csv_writer.writeheader()
            csv_writer.writerows(weather_station_data)

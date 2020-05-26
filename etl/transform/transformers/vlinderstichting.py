from etl.transform.transformers.base import Base
from pathlib import Path
from datetime import datetime
import csv

class Vlinderstichting(Base):

    def transform(self, extract_directory, transform_directory):
        file_path = extract_directory / 'epr_20200521.csv'

        with open(file_path) as input_file:
            csv_reader = csv.DictReader(input_file, delimiter=';')

            # Note: coordinates are in eps:28992

            oak_processionary_moths = [dict(
                date=datetime.date(row['"jaar"'], row['"maand"'], row['"dag"']),
                stage=row['stadium'],
                geometry=row['']
            ) for row in csv_reader]






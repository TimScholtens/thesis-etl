from etl.transform.transformers.base import Base
from pathlib import Path

class BioClim_1(Base):

    def transform(self, extract_directory, transform_directory):
        # Calculate centroids for each township
        townships_file_path = Path(extract_directory) / 'townships.json'

        with open(townships_file_path) as townships_file:
            pass

        # Filter column 'average temperature' for each weather station

        # Calculate yearly average temperature for each weather station

        # Interpolate for each township the annual temperature
        """
        Fit k-nearest-neigbors ball tree having parameters:
         - For interpolation use Inverse Distance Weighting to determine weights of each weather station
         - For interpolation use Haversine to calculate distance between given point and weather station
        """




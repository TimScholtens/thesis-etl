import unittest
from etl.transform.transformers.bioclim import BioClim1TimePartitionStrategy, get_weather_station_values
from pathlib import Path


class BioClimTransformerTestCases(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Load test data for station ID 242, for the year 2000.
        extract_directory = Path.cwd() / 'static'
        cls.weather_station_values = get_weather_station_values(extract_directory)

    @classmethod
    def tearDownClass(cls):
        cls.weather_station_values = None

    def test_bioclim1_time_partition_strategy(self):
        """
            Strategy must return mean annual temperature.
        """
        df_year = BioClim1TimePartitionStrategy().aggregate(self.weather_station_values)
        expected_average_temp = 12.77020408  # From spreadsheet in google drive
        calculated_average_temp = df_year['temperature_avg'].values[0]

        # Compare if values are correct up to 4 decimals
        self.assertAlmostEqual(expected_average_temp, calculated_average_temp, 4)


if __name__ == '__main__':
    unittest.main()

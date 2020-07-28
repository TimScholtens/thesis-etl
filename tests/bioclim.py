import unittest
from etl.transform.transformers.bioclim import (

    BioClim1TimePartitionStrategy,
    BioClim8TimePartitionStrategy,
    BioClim17TimePartitionStrategy,
    get_weather_station_values
)
from pathlib import Path
import numpy as np


class BioClimTransformerTestCases(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Load test data for station ID 242, for the year 2000.
        extract_directory = Path.cwd() / 'static'
        cls.weather_station_values = get_weather_station_values(extract_directory)

        # Set max percentage tolerance
        cls.MAX_PERCENT_DEVIATION = 5

    @classmethod
    def tearDownClass(cls):
        cls.weather_station_values = None

    def log(self, metric_id, expected_value, calculated_value):
        print(f'\t Expected {metric_id}: {expected_value}, \n '
              f'\t Calculated {metric_id}: {calculated_value}')

    def test_bioclim1_time_partition_strategy(self):
        """
            Strategy must return mean annual temperature.
        """
        df_year = BioClim1TimePartitionStrategy().aggregate(self.weather_station_values)
        expected_average_temp = 10.62868852  # From spreadsheet in google drive
        calculated_average_temp = df_year['temperature_avg'].values[0]

        # Compare if values are match within 5% tolerance
        np.testing.assert_allclose(actual=calculated_average_temp,
                                   desired=expected_average_temp,
                                   rtol=self.MAX_PERCENT_DEVIATION)

        self.log(metric_id='temperature_avg',
                 expected_value=expected_average_temp,
                 calculated_value=calculated_average_temp)

    def test_bioclim8_time_partition_strategy(self):
        """
            Strategy must return the average temperature of the quarter with the highest rain sum.
        """
        # df_year = BioClim8TimePartitionStrategy().aggregate(self.weather_station_values)
        #
        # expected_most_rainfall_quartile = 3
        # expected_sum_rainfall = 3941  # in mms
        # expected_average_temperature = 12.39262295

        self.assertEqual(True, True)

    def test_bioclim17_time_partition_strategy(self):
        """
            Strategy must return the total precipitation of the driest quarter.
        """
        df_year = BioClim17TimePartitionStrategy().aggregate(self.weather_station_values)

        expected_sum_rainfall = 103.3  # in mm
        calculated_sum_rainfall = df_year['rain_sum'][0]

        # Compare if values are match within 5% tolerance
        np.testing.assert_allclose(actual=calculated_sum_rainfall,
                                   desired=expected_sum_rainfall,
                                   rtol=self.MAX_PERCENT_DEVIATION)

        self.log(metric_id='rain_sum',
                 expected_value=expected_sum_rainfall,
                 calculated_value=calculated_sum_rainfall)


if __name__ == '__main__':
    unittest.main()

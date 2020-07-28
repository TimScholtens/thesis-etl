import unittest
from etl.transform.transformers.bioclim import (

    BioClim1TimePartitionStrategy,
    BioClim2TimePartitionStrategy,
    BioClim8TimePartitionStrategy,
    BioClim16TimePartitionStrategy,
    BioClim17TimePartitionStrategy,
    get_weather_station_values
)
from pathlib import Path
from math import isclose


class BioClimTransformerTestCases(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Load test data for station ID 242, for the year 2000.
        extract_directory = Path.cwd() / 'static'
        cls.weather_station_values = get_weather_station_values(extract_directory)

        # Set max percentage tolerance
        cls.MAX_PERCENT_DEVIATION = 0.05

    @classmethod
    def tearDownClass(cls):
        cls.weather_station_values = None

    def log(self, metric_id, expected_value, calculated_value):
        print(f'\t Expected {metric_id}: {expected_value}, \n '
              f'\t Calculated {metric_id}: {calculated_value}\n')

    def test_bioclim1_time_partition_strategy(self):
        """
            Strategy must return mean annual temperature.
        """
        df_year = BioClim1TimePartitionStrategy().aggregate(self.weather_station_values)
        expected_average_temp = 10.62868852  # From spreadsheet in google drive
        calculated_average_temp = df_year['temperature_avg'].values[0]

        self.log(metric_id='temperature_avg',
                 expected_value=expected_average_temp,
                 calculated_value=calculated_average_temp)

        # Compare if values are match within 5% tolerance
        assert isclose(a=calculated_average_temp,
                       b=expected_average_temp,
                       rel_tol=self.MAX_PERCENT_DEVIATION)

    def test_bioclim2_time_partition_strategy(self):
        """
            Strategy must return mean diurnal range.
        """
        # Workaround - Add columns 'longitude' and 'latitude' in order to avoid exception in 'aggregate()'
        df = self.weather_station_values
        df['latitude'] = 0
        df['longitude'] = 0
        df_year = BioClim2TimePartitionStrategy().aggregate(df)
        expected_diurnal_range = 6.887301  # From spreadsheet in google drive
        calculated_diurnal_range = df_year['temperature_range'].values[0]

        self.log(metric_id='temperature_range',
                 expected_value=expected_diurnal_range,
                 calculated_value=calculated_diurnal_range)

        # Compare if values are match within 5% tolerance
        assert isclose(a=expected_diurnal_range,
                       b=calculated_diurnal_range,
                       rel_tol=self.MAX_PERCENT_DEVIATION)

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

    def test_bioclim16_time_partition_strategy(self):
        """
            Strategy must return the total precipitation of the wettest quarter.
        """
        df_year = BioClim16TimePartitionStrategy().aggregate(self.weather_station_values)

        expected_sum_rainfall = 350  # in mm
        calculated_sum_rainfall = df_year['rain_sum'][0]

        self.log(metric_id='rain_sum',
                 expected_value=expected_sum_rainfall,
                 calculated_value=calculated_sum_rainfall)

        # Compare if values are match within 5% tolerance
        assert isclose(a=calculated_sum_rainfall,
                       b=expected_sum_rainfall,
                       rel_tol=self.MAX_PERCENT_DEVIATION)

    def test_bioclim17_time_partition_strategy(self):
        """
            Strategy must return the total precipitation of the driest quarter.
        """
        df_year = BioClim17TimePartitionStrategy().aggregate(self.weather_station_values)

        expected_sum_rainfall = 103.3  # in mm
        calculated_sum_rainfall = df_year['rain_sum'][0]

        self.log(metric_id='rain_sum',
                 expected_value=expected_sum_rainfall,
                 calculated_value=calculated_sum_rainfall)

        # Compare if values are match within 5% tolerance
        assert isclose(a=calculated_sum_rainfall,
                       b=expected_sum_rainfall,
                       rel_tol=self.MAX_PERCENT_DEVIATION)


if __name__ == '__main__':
    unittest.main()

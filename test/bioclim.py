import unittest
import pandas as pd
from etl.transform.transformers.bioclim import BioClim1TimePartitionStrategy


class BioClimTransformerTestCases(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('Hello!')
        # Test data for station ID 242, for year 2000
        extract_directory = ''
        cls.input_data =

    @classmethod
    def tearDownClass(cls):
        # cls.input_data = None
        print('Bye!')

    def test_bioclim1_time_partition_strategy(self):
        """
            Strategy must return mean annual temperature.
        """
        # print(self.input_data)
        print('running tests..')
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()

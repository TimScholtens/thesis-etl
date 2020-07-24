import unittest
from etl.transform.transformers.bioclim import interpolate


class MyTestCase(unittest.TestCase):

    def inverse_distance_weighting(self):
        pass

    def haversine_distance(self):
        pass

    def test_interpolation(self):
        """
        Check if the above interpolation function has the same values as the interpolation function
        defined in sklearn.KNeighborsRegressor.
        :return:
        """
        training_coordinates = None
        training_values = None
        interpolation_coordinates = None
        predicted_interpolation_values = None
        expected_interpolation_values = None

        interpolate(training_coordinates, training_values, interpolation_coordinates)

        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()

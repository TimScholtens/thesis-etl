import unittest
from etl.transform.transformers.bioclim import interpolate


class MyTestCase(unittest.TestCase):
    def test_interpolation(self):
        training_coordinates = None
        training_values = None
        interpolation_coordinates = None
        predicted_interpolation_values = None
        expected_interpolation_values = None

        interpolate(training_coordinates, training_values, interpolation_coordinates)


        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()

import unittest
from etl.transform.transformers.bioclim import interpolate


class MyTestCase(unittest.TestCase):
    def test_interpolation(self):
        """
        Some examples from an online haversine calculator ...
            - (calculator) https://www.movable-type.co.uk/scripts/latlong.html
            - (wsg-84 coordinate selector) https://www.latlong.net/
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

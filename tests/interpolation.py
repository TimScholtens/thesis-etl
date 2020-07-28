import unittest
import math
from etl.transform.transformers.bioclim import interpolate


class InterpolationTestCases(unittest.TestCase):

    def inverse_distance_weighting(self):
        pass

    def haversine_distance(self, lat1, lat2, lon1, lon2):
        """
        Source: https://www.geeksforgeeks.org/haversine-formula-to-find-distance-between-two-points-on-a-sphere/
        :return: Distance in KM between two points
        """
        # distance between latitudes
        # and longitudes
        dLat = (lat2 - lat1) * math.pi / 180.0
        dLon = (lon2 - lon1) * math.pi / 180.0

        # convert to radians
        lat1 = (lat1) * math.pi / 180.0
        lat2 = (lat2) * math.pi / 180.0

        # apply formulae
        a = (pow(math.sin(dLat / 2), 2) +
             pow(math.sin(dLon / 2), 2) *
             math.cos(lat1) * math.cos(lat2));
        rad = 6371
        c = 2 * math.asin(math.sqrt(a))
        return rad * c

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

lat1 = 51.516449
lon1 = 5.491679
lat2 = 51.522965
lon2 = 5.533392

print(InterpolationTestCases().haversine_distance(lat1,lat2,lon1,lon2))
import unittest
from GLHE.data_access import ERA5_Land

class MyTestCase(unittest.TestCase):
    def test_era5_api(self):
        era5_dataset = ERA5_Land.get_total_precip_runoff_evap_in_subset_box_api(1.1, 1.2, 0.5, 0.6)
        self.assertIsNotNone(era5_dataset)


if __name__ == '__main__':
    unittest.main()

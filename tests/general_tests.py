import os
import unittest
import sys
from pint import UnitRegistry
import GLHE.globals

import numpy as np
import pandas as pd
import xarray as xr
import fsspec
import logging
import GLHE
from GLHE import helpers, lake_extraction, combined_data_functions, xarray_helpers
from GLHE.data_access import ERA5_Land, CRUTS, NWM, data_check


class BaseTestCase(unittest.TestCase):

    def setUp(self):
        os.chdir(r'C:\Users\manis\OneDrive - Umich\Documents\Global Lake Hydrology Explorer\GLHE')

        self.terra_dataset = xr.load_dataset(
            r'C:\Users\manis\OneDrive - Umich\Documents\Global Lake Hydrology '
            r'Explorer\tests\DummyData\agg_terraclimate_ppt_1958_CurrentYear_GLOBE.nc')
        self.terra_dataset = xarray_helpers.label_xarray_dataset_with_product_name(self.terra_dataset,
                                                                                   "TerraClimateDummyData")
        dims = ('time', 'lat', 'lon')
        time = pd.date_range("2021-01-01T00", "2023-12-31T23", freq="H")
        lat = [0, 1]
        lon = [0, 1]
        coords = (time, lat, lon)
        self.random_dataset = xr.DataArray(data=np.random.randn(len(time), len(lat), len(lon)), coords=coords,
                                           dims=dims).rename(
            "my_var").to_dataset()


class MyTestCase(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        helpers.setup_logging_directory(
            r'C:\Users\manis\OneDrive - Umich\Documents\Global Lake Hydrology Explorer\GLHE\.temp')
        logging.basicConfig(
            encoding='utf-8', level=os.environ.get("LOGLEVEL", "INFO"),
            format='%(asctime)s.%(msecs)03d %(levelname)s: %(module)s.%(funcName)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S', )
        logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
        logger = logging.getLogger(__name__)
        fh = logging.FileHandler(os.path.join(GLHE.globals.LOGGING_DIRECTORY, "GLHE_root_test.log"))
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s: %(module)s.%(funcName)s: %(message)s')
        fh.setFormatter(formatter)
        logging.getLogger().addHandler(fh)
        logger.info("***********************Initializing Test Functions*************************")
        helpers.setup_output_directory("TestLake")
        GLHE.globals.LAKE_NAME = "TestLake"

    def test_name(self):
        self.terra_dataset = xarray_helpers.label_xarray_dataset_with_product_name(self.terra_dataset,
                                                                                   "TerraClimateDummyData")
        self.assertEqual(self.terra_dataset.attrs['product_name'], "TerraClimateDummyData")

    def test_unit_conversion(self):
        self.terra_dataset = xarray_helpers.convert_xarray_dataset_units(self.terra_dataset, "in", "ppt")
        self.assertIsNotNone(self.terra_dataset)

    def test_xarray_untis_conversion_stuff(self):
        dataset = helpers.unpickle_var("NWM")
        dataset = xarray_helpers.rename_xarray_units(dataset, "m3/s", "inflow", "outflow")
        dataset = xarray_helpers.convert_xarray_dataset_units(dataset, "m3/month", "inflow",
                                                              "outflow")

    def test_lake_extraction(self):
        polygon = lake_extraction.extract_lake(798)
        self.assertIsNotNone(polygon)

    def test_subset_box(self):
        polygon = lake_extraction.extract_lake(798)
        self.terra_dataset = lake_extraction.subset_box(self.terra_dataset, polygon, 1)
        self.assertIsNotNone(self.terra_dataset)

    def test_spatially_average_dataset(self):
        polygon = lake_extraction.extract_lake(798)
        self.terra_dataset = lake_extraction.subset_box(self.terra_dataset, polygon, 1)
        self.assertIsNotNone(xarray_helpers.spatially_average_xarray_dataset_and_convert(self.terra_dataset, "ppt"))

    def test_plotting(self):
        csv_dataset = pd.read_csv(
            r'C:\Users\manis\OneDrive - Umich\Documents\Global Lake Hydrology Explorer\GLHE\.temp\test.csv')
        csv_dataset['date'] = pd.to_datetime(csv_dataset['date'])
        csv_dataset.set_index('date', inplace=True)
        combined_data_functions.output_plot_of_all_data(csv_dataset)
        self.assertEqual(1, 1)

    def test_nwm_find_lake_id(self):
        polygon = lake_extraction.extract_lake(61)
        NWM.find_lake_id(polygon)
        self.assertEqual(1, 1)

    def test_download_data_check(self):
        list_of_files = data_check.check_data_and_download_missing_data_or_files()
        self.assertEqual(1, 1)

    def test_ERA5_class(self):
        ERA5_object = ERA5_Land.ERA5_Land()
        polygon = lake_extraction.extract_lake(61)
        ERA5_object.product_driver(polygon)
        self.assertEqual(1, 1)

    def test_NWM_class(self):
        lake_extraction_object = lake_extraction.LakeExtraction()
        lake_extraction_object.extract_lake_information(67)
        polygon = lake_extraction_object.get_lake_polygon()
        print(lake_extraction_object.get_lake_name())
        NWM_object = NWM.NWM()
        NWM_object.product_driver(polygon, True, True)
        self.assertEqual(1, 1)

    def test_NWM_zarr(self):
        lake_extraction_object = lake_extraction.LakeExtraction()
        lake_extraction_object.extract_lake_information(67)
        polygon = lake_extraction_object.get_lake_polygon()
        NWM_object = NWM.NWM()
        NWM_object.zarr_test_work(polygon, 1204)
        self.assertEqual(1, 1)

    def test_NWM_unit_converter(self):
        NWM_object = NWM.NWM()
        NWM_object.check_save_file()
        data = pd.DataFrame(NWM_object.full_data)
        helpers.convert_MVSeries_units([data['inflow']], "m3/month")


if __name__ == '__main__':
    unittest.main()

from GLHE.CLAY.data_access import ERA5_Land, NWM, CRUTS, data_check
import pytest
import shapely.geometry
import GLHE.CLAY.lake_extraction as lake_extraction
import GLHE


class TestDataAccessInitialization:

    @pytest.mark.slow
    def test_download_data_check(self):
        list_of_files = data_check.check_data_and_download_missing_data_or_files()
        assert list_of_files == None

    def test_ERA5_Land(self):
        era5_land = ERA5_Land.ERA5_Land()
        assert era5_land.verify_inputs() == True

    def test_NWM(self):
        nwm = NWM.NWM()
        assert nwm.verify_inputs() == True

    def test_CRUTS(self):
        cruts = CRUTS.CRUTS()
        assert cruts.verify_inputs() == True


class TestERA5Land:

    lake_polygon: shapely.geometry.Polygon
    lake_id: int

    @classmethod
    def setup_class(self):
        """setup any state specific to the execution of the given class (which
        usually contains tests).
        """
        self.lake_id = 63
        self.grab_sample_lake_polygon(self)
        GLHE.CLAY.helpers.setup_output_directory("TestLake")
        GLHE.CLAY.helpers.setup_logging_directory(".temp_tests")

    def grab_sample_lake_polygon(self):
        lake_extraction_object = lake_extraction.LakeExtraction()
        lake_extraction_object.extract_lake_information(self.lake_id)
        GLHE.CLAY.globals.config["LAKE_NAME"] = lake_extraction_object.get_lake_name()
        self.lake_polygon = lake_extraction_object.get_lake_polygon()

    @pytest.mark.subsetting_data
    def test_ERA5_Land(self):
        era5_land = ERA5_Land.ERA5_Land()
        era5_data = era5_land.product_driver(
            self.lake_polygon, debug=False, run_cleanly=False
        )
        assert era5_data != None


class TestNWM:

    lake_polygon: shapely.geometry.Polygon
    lake_id: int

    @classmethod
    def setup_class(self):
        """setup any state specific to the execution of the given class (which
        usually contains tests).
        """
        self.lake_id = 67
        self.grab_sample_lake_polygon(self)
        GLHE.CLAY.helpers.setup_output_directory("TestLake")
        GLHE.CLAY.helpers.setup_logging_directory(".temp_tests")

    def grab_sample_lake_polygon(self):
        lake_extraction_object = lake_extraction.LakeExtraction()
        lake_extraction_object.extract_lake_information(self.lake_id)
        GLHE.CLAY.globals.config["LAKE_NAME"] = lake_extraction_object.get_lake_name()
        self.lake_polygon = lake_extraction_object.get_lake_polygon()

    @pytest.mark.subsetting_data
    def test_NWM(self):
        nwm = NWM.NWM()
        nwm_data = nwm.product_driver(self.lake_polygon, debug=False, run_cleanly=False)
        assert nwm_data != None

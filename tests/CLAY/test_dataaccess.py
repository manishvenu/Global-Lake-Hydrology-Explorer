from GLHE.CLAY.data_access import ERA5_Land, NWM, CRUTS, data_check
import pytest


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

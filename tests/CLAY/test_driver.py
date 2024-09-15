import pytest
import GLHE.CLAY.lake_extraction
from GLHE.CLAY import CLAY_driver


class TestDriver:

    @classmethod
    def setup_class(self):
        """setup any state specific to the execution of the given class (which
        usually contains tests).
        """
        GLHE.CLAY.helpers.setup_logging_directory(".temp_tests")

    def test_clay_driver_init(self):
        clay_driver = CLAY_driver.CLAY_driver()
        assert clay_driver.lake_polygon == None

    def test_lake_extraction(self):
        lake_extraction = GLHE.CLAY.lake_extraction.LakeExtraction()
        Mono_Hylak_ID = 798
        lake_extraction.extract_lake_information(Mono_Hylak_ID)
        assert lake_extraction.get_lake_name() == "Mono_Lake"

    def test_data_access_initialization_handler(self):
        clay_driver = CLAY_driver.CLAY_driver()
        data_products = clay_driver.load_data_product_list()
        assert len(data_products) > 0


@pytest.mark.slow
def test_run():
    clay_driver = CLAY_driver.CLAY_driver()
    GSL_Hylak_ID = 67
    res = clay_driver.main(GSL_Hylak_ID)
    assert True

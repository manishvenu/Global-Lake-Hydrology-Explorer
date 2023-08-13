import logging
import os
import sys
import pandas as pd

import GLHE.globals
from GLHE import lake_extraction, combined_data_functions, helpers
from GLHE.data_access import ERA5_Land, CRUTS, NWM, data_check


class driver:
    """
    This is the driver class.
    """
    datasets_index = {
        "all": [],
        "grid": [],
        "slc": {}
    }
    pandas_dataset: pd.DataFrame
    root_logger: logging.Logger
    data_products = {}

    def __init__(self):
        """
        This is the constructor for the driver class.
        """
        for key in GLHE.globals.SLC_MAPPING_REVERSE_NAMES:
            self.datasets_index["slc"][key] = []

    def index_datasets(self, *datasets: helpers.MVSeries):
        """
        This function indexes the datasets by their SLCs.
        """

        for ds in self.datasets_index["all"]:
            # By Slc
            self.datasets_index["slc"][ds.single_letter_code].append(ds)

    def set_up_logging(self) -> None:
        helpers.setup_logging_directory(".temp")
        logging.basicConfig(
            encoding='utf-8', level=os.environ.get("LOGLEVEL", "INFO"),
            format='%(asctime)s.%(msecs)03d %(levelname)s: %(module)s.%(funcName)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S', )
        fh = logging.FileHandler(os.path.join(GLHE.globals.LOGGING_DIRECTORY, "GLHE_root.log"))
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s: %(module)s.%(funcName)s: %(message)s')
        fh.setFormatter(formatter)
        self.root_logger = logging.getLogger()
        self.root_logger.addHandler(fh)
        self.root_logger.addHandler(logging.StreamHandler(sys.stdout))

    def main(self) -> None:
        """
        This is the driver function.
        Here, we'll run all the code.
        """

        # Set up logging #
        self.set_up_logging()
        self.root_logger.info("***********************Initializing Driver Function*************************")
        # Check if Debug Mode is On #
        if GLHE.globals.DEBUG:
            self.root_logger.info("Debug Mode is On")
        # Verify access to data #
        data_check.check_data_and_download_missing_data_or_files()

        # Access lake information#
        HYLAK_ID = 67
        lake_extraction_object = lake_extraction.LakeExtraction()
        lake_extraction_object.extract_lake_information(HYLAK_ID)
        GLHE.globals.LAKE_NAME = lake_extraction_object.get_lake_name()

        helpers.setup_output_directory(GLHE.globals.LAKE_NAME)

        # Transition to new logging directory
        helpers.setup_logging_directory(os.path.join(GLHE.globals.LAKE_OUTPUT_FOLDER, GLHE.globals.LAKE_NAME))

        # Get lake polygon #
        lake_polygon = lake_extraction_object.get_lake_polygon()

        # Set up data accessers #
        self.data_products = {
            "ERA5_Land": {
                "object": ERA5_Land.ERA5_Land(),
                "output_datasets": [],
                "README": "Validate this data with the gridded geodata in the zip file"
            },
            "CRUTS": {
                "object": CRUTS.CRUTS(),
                "output_datasets": [],
                "README": "Validate this data with the gridded geodata in the zip file",
            },
            "NWM": {
                "object": NWM.NWM(),
                "output_datasets": [],
                "README": "Validate this data with the point file labeled 'NWM' in the output folder",
            }
        }

        # Collect the data #
        for key in self.data_products:
            try:
                self.data_products[key]["output_datasets"] = (
                    self.data_products[key]["object"].product_driver(lake_polygon, GLHE.globals.DEBUG,
                                                                     GLHE.globals.RUN_CLEANLY))

                for ds in self.data_products[key]["output_datasets"]:
                    self.root_logger.info(ds)
                self.datasets_index["all"].extend(self.data_products[key]["output_datasets"])
            except Exception as e:
                print("Data Product: {} not available for this lake with Exception: {}".format(key, e))

        # Attach Geodata
        for key in self.data_products:
            response = self.data_products[key]["object"].attach_geodata()
            if response == "grid":
                self.datasets_index["grid"].extend(self.data_products[key]["output_datasets"])
            elif response == "complete":
                pass
            else:
                raise ValueError("Response from attach_geodata() must be either 'grid' or 'complete'")

        combined_data_functions.present_mv_series_as_geospatial_at_date_time(pd.to_datetime('2002-05-01'),
                                                                             *self.datasets_index["grid"])

        # Plot and Output
        self.index_datasets()  # Required for datasets_index: slc
        self.pandas_dataset = combined_data_functions.merge_mv_series_into_pandas_dataframe(self.datasets_index["slc"])
        combined_data_functions.output_plot_of_all_data(self.pandas_dataset)
        combined_data_functions.output_all_compiled_data_to_csv(GLHE.globals.LAKE_NAME + "_Data.csv",
                                                                self.pandas_dataset)
        combined_data_functions.write_and_output_README(self.data_products)
        logging.info('Ended driver function')


if __name__ == "__main__":
    driver_obj = driver()
    driver_obj.main()

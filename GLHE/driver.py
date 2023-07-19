import logging
import os
import sys
import asyncio
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
    outputs = {}

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

        for ds in datasets:
            # By All
            self.datasets_index["all"].append(ds)
            # By Slc
            self.datasets_index["slc"][ds.single_letter_code].append(ds)
            # By Grid
            if ds.xarray_dataarray is not None:
                self.datasets_index["grid"].append(ds)

        # By Row

    async def main(self) -> None:
        """
        This is the driver function.
        Here, we'll run all the code.
        """

        helpers.setup_logging_directory()
        logging.basicConfig(
            encoding='utf-8', level=os.environ.get("LOGLEVEL", "INFO"),
            format='%(asctime)s.%(msecs)03d %(levelname)s: %(module)s.%(funcName)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S', )
        fh = logging.FileHandler(os.path.join(GLHE.globals.LOGGING_DIRECTORY, "GLHE_driver.log"))
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s: %(module)s.%(funcName)s: %(message)s')
        fh.setFormatter(formatter)
        self.root_logger = logging.getLogger()
        self.root_logger.addHandler(fh)

        self.root_logger.addHandler(logging.StreamHandler(sys.stdout))
        self.root_logger.info("***********************Initializing Driver Function*************************")
        # Check if Debug Mode is On #
        if GLHE.globals.DEBUG:
            self.root_logger.info("Debug Mode is On")
        # Verify access to data #
        data_check.check_data_and_download_missing_data_or_files()

        # Access lake information and setup output directory#
        HYLAK_ID = 67
        lake_polygon = lake_extraction.extract_lake(HYLAK_ID)
        helpers.setup_output_directory(GLHE.globals.LAKE_NAME)

        # Initialize data access classes and get data #
        ERA5_object = ERA5_Land.ERA5_Land()
        CRUTS_object = CRUTS.CRUTS()
        NWM_object = NWM.NWM()

        ERA5_task = asyncio.create_task(ERA5_object.product_driver(lake_polygon, GLHE.globals.DEBUG))
        CRUTS_Datasets = CRUTS_object.product_driver(lake_polygon, GLHE.globals.DEBUG)
        for i in CRUTS_Datasets:
            logging.info(i)

        try:
            NWM_Datasets = NWM_object.product_driver(lake_polygon, GLHE.globals.DEBUG)
        except Exception as e:
            logging.info("NWM Data not available for this lake")
            NWM_Datasets = []
        for i in NWM_Datasets:
            logging.info(i)

        ERA5_Datasets = await ERA5_task
        for i in ERA5_Datasets:
            logging.info(i)

        self.index_datasets(*ERA5_Datasets, *CRUTS_Datasets, *NWM_Datasets)

        # Compile Data
        combined_data_functions.present_mv_series_as_geospatial_at_date_time(pd.to_datetime('2002-05-01'),
                                                                             *self.datasets_index["grid"])

        # Plot and Output
        self.pandas_dataset = combined_data_functions.merge_mv_series_into_pandas_dataframe(self.datasets_index["slc"])
        combined_data_functions.output_plot_of_all_data(self.pandas_dataset)
        combined_data_functions.output_all_compiled_data_to_csv(GLHE.globals.LAKE_NAME + "_Data.csv",
                                                                self.pandas_dataset)

        combined_data_functions.write_and_output_README(self.outputs)
        logging.info('Ended driver function')


if __name__ == "__main__":
    driver_obj = driver()
    asyncio.run(driver_obj.main())

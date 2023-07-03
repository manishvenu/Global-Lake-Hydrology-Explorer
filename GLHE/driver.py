import logging
import os
import sys
import asyncio
import pandas as pd

import GLHE.globals
from GLHE import lake_extraction, combined_data_functions, helpers
from GLHE.data_access import ERA5_Land, CRUTS, NWM, data_check


async def driver() -> None:
    """
    This is the driver function.
    Here, we'll run all the code.
    """

    # Set up logging
    logging.basicConfig(
        filename='C:\\Users\\manis\\OneDrive - Umich\\Documents\\Global Lake Hydrology Explorer\\GLHE\\.temp\\GLHE.log',
        encoding='utf-8', level=os.environ.get("LOGLEVEL", "INFO"),
        format='%(asctime)s.%(msecs)03d %(levelname)s: %(module)s.%(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S', )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    logging.info("***********************Starting Driver Function*************************")

    # Verify access to data #
    data_check.check_data_and_download_missing_data_or_files()

    # Access lake information and setup output directory#
    HYLAK_ID = 61
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

    NWM_Datasets = NWM_object.product_driver(lake_polygon, GLHE.globals.DEBUG)
    for i in NWM_Datasets:
        logging.info(i)

    ERA5_Datasets = await ERA5_task
    for i in ERA5_Datasets:
        logging.info(i)

    # Compile Data
    pandas_dataset = combined_data_functions.merge_mv_series_into_pandas_dataframe(*ERA5_Datasets, *CRUTS_Datasets)
    combined_data_functions.present_mv_series_as_geospatial_at_date_time(pd.to_datetime('2002-05-01'), *ERA5_Datasets,
                                                                         *CRUTS_Datasets)

    # Plot and Output
    combined_data_functions.plot_all_data(pandas_dataset)
    combined_data_functions.output_all_compiled_data_to_csv(pandas_dataset, GLHE.globals.LAKE_NAME + "_Data.csv")
    logging.info('Ended driver function')


if __name__ == "__main__":
    asyncio.run(driver())

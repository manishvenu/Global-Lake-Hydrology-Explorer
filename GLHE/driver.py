import sys
from GLHE import lake_extraction, product_driver_functions, combined_data_functions
import logging
import os

def driver() -> None:
    """
    This is the driver function for the combined_data_functions.py file.
    Here, we'll access all the data
    """
    # Set up logging
    logging.basicConfig(filename='GLHE.log', encoding='utf-8', level=os.environ.get("LOGLEVEL", "INFO"))
    logging.info("Starting driver function")
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    # Select Lake and Extract Polygon
    hylak_id = 798
    lake_polygon = lake_extraction.extract_lake(hylak_id)

    # Get Data and subset it to lake polygon
    ERA5_Datasets = product_driver_functions.ERA_Land_driver(lake_polygon)
    for i in ERA5_Datasets:
        logging.info(i)
    CRUTS_Datasets = product_driver_functions.CRUTS_driver(lake_polygon)
    for i in CRUTS_Datasets:
        logging.info(i)

    # Compile Data
    pandas_dataset = combined_data_functions.merge_mv_series(*ERA5_Datasets, *CRUTS_Datasets)

    # Plot and Output
    combined_data_functions.plot_all_data(pandas_dataset)
    combined_data_functions.output_all_compiled_data_to_csv(pandas_dataset, "test.csv")

    logging.info('Ended driver function')
if __name__ == "__main__":
    driver()

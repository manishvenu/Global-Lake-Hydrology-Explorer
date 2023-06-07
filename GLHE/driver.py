import logging
import os
import sys

from GLHE import lake_extraction, product_driver_functions, combined_data_functions


def driver() -> None:
    """
    This is the driver function.
    Here, we'll run all the code.
    """

    # Set up logging
    logging.basicConfig(filename='.temp/GLHE.log', encoding='utf-8', level=os.environ.get("LOGLEVEL", "INFO"))
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.info("Starting driver function")
    # Select Lake and Extract Polygon
    hylak_id = 798
    lake_polygon = lake_extraction.extract_lake(hylak_id)

    # Get Data and subset it to lake polygon
    ERA5_Datasets = product_driver_functions.ERA_Land_driver(lake_polygon,True)
    for i in ERA5_Datasets:
        logging.info(i)
    CRUTS_Datasets = product_driver_functions.CRUTS_driver(lake_polygon,True)
    for i in CRUTS_Datasets:
        logging.info(i)

    # Compile Data
    pandas_dataset = combined_data_functions.merge_mv_series_into_pandas_dataframe(*ERA5_Datasets, *CRUTS_Datasets)
    combined_data_functions.present_mv_series_as_geopackage("test1.gpkg",*ERA5_Datasets, *CRUTS_Datasets)
    # Plot and Output
    combined_data_functions.plot_all_data(pandas_dataset)
    combined_data_functions.output_all_compiled_data_to_csv(pandas_dataset, "test.csv")

    logging.info('Ended driver function')


if __name__ == "__main__":
    driver()

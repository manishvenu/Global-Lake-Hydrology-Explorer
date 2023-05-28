# Functions from here are required to return Pandas Series w/ MetaData

from GLHE.data_access import ERA5_Land
from GLHE import helpers, lake_extraction
import pandas as pd


class mv_series:
    # Class Variable
    dataset: pd.Series
    unit: str
    single_letter_code: str
    product_name: str

    def __init__(self, dataset, unit,single_letter_code, name):
        self.dataset = dataset
        self.unit = unit
        self.single_letter_code = single_letter_code
        self.name = name

    def __str__(self):
        return f"Product: {self.name}, SLC: {self.single_letter_code}, Unit: {self.unit}"

    # Change Units MetaData DOESNT CHANGE ACTUAL VALUES
    def setUnits(self, unit):
        self.unit = unit


def ERA_Land_Driver(polygon) -> list[mv_series]:
    """
    Returns ERA5 Land Data for a given lake using functions from helpers and data_access

            Parameters
            ----------
            polygon : shapely.geometry.Polygon
                The polygon of the lake
            Returns
            -------
            mv_series
                Pandas Series with MetaData
    """

    min_lon, min_lat, max_lon, max_lat = polygon.bounds

    ## Need to more to add more padding to lat, longs ##
    min_lon = min_lon - 1
    max_lon = max_lon + 1
    min_lat = min_lat - 1
    max_lat = max_lat + 1

    dataset = ERA5_Land.get_total_precip_runoff_evap_in_subset_box_api(min_lon, max_lon, min_lat, max_lat)
    dataset = helpers.fix_lat_long_names(dataset)
    dataset = lake_extraction.subset_box(dataset, polygon, 1)
    evap_ds = helpers.spatially_average_dataset(dataset, "e")
    precip_ds = helpers.spatially_average_dataset(dataset, "tp")
    helpers.clean_up_temporary_files()
    list_of_mv_series = [mv_series(evap_ds, dataset.variables['e'].attrs['units'], "e", "ERA5 Land"), mv_series(precip_ds, dataset.variables['tp'].attrs['units'], "p", "ERA5 Land")]
    return list_of_mv_series


if __name__ == "__main__":
    for i in ERA_Land_Driver(lake_extraction.extract_lake(798)):
        print(i)

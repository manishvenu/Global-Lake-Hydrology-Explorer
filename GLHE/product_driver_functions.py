# Functions from here are required to return Pandas Series w/ MetaData

from GLHE.data_access import ERA5_Land, CRUTS
from GLHE.helpers import MVSeries
from GLHE import helpers, lake_extraction


def ERA_Land_driver(polygon) -> list[MVSeries]:
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

    # Need to more to add more padding to lat, longs ##
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
    list_of_mv_series = [MVSeries(evap_ds, dataset.variables['e'].attrs['units'], "e", "ERA5 Land"),
                         MVSeries(precip_ds, dataset.variables['tp'].attrs['units'], "p", "ERA5 Land")]
    return list_of_mv_series


def CRUTS_driver(polygon) -> list[MVSeries]:
    """
    Returns CRUTS Data for a given lake using functions from helpers and data_access

            Parameters
            ----------
            polygon : shapely.geometry.Polygon
                The polygon of the lake
            Returns
            -------
            mv_series
                Pandas Series with MetaData
    """

    dataset = CRUTS.get_total_precip_evap()
    dataset = helpers.fix_lat_long_names(dataset)
    dataset = lake_extraction.subset_box(dataset, polygon.buffer(0.5), 1)
    pet_ds = helpers.spatially_average_dataset(dataset, "pet")
    precip_ds = helpers.spatially_average_dataset(dataset, "pre")
    helpers.clean_up_temporary_files()
    list_of_mv_series = [MVSeries(pet_ds, dataset.variables['pet'].attrs['units'], "pet", "CRUTS"),
                         MVSeries(precip_ds, dataset.variables['pre'].attrs['units'], "p", "CRUTS")]
    return list_of_mv_series


if __name__ == "__main__":
    print("Driver Functions Accessed Here")

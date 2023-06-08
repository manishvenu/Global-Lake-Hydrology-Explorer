# Functions from here are required to return Pandas Series w/ MetaData

import logging

from GLHE import helpers, lake_extraction
from GLHE.data_access import ERA5_Land, CRUTS
from GLHE.helpers import MVSeries

logger = logging.getLogger(__name__)


def ERA_Land_driver(polygon, debug=False) -> list[MVSeries]:
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
    logger.info("ERA Driver Started: Precip & Evap")
    if not debug:
        min_lon, min_lat, max_lon, max_lat = polygon.bounds

        # Need to more to add more padding to lat, longs ##
        min_lon = min_lon - 1
        max_lon = max_lon + 1
        min_lat = min_lat - 1
        max_lat = max_lat + 1

        dataset = ERA5_Land.get_total_precip_runoff_evap_in_subset_box_api(min_lon, max_lon, min_lat, max_lat)
        dataset = helpers.label_xarray_dataset_with_product_name(dataset, "ERA5 Land")
        dataset = helpers.fix_lat_long_names(dataset)
        try:
            dataset = lake_extraction.subset_box(dataset, polygon, 1)
        except ValueError:
            logging.error(
                "ERA5 Labd subset Failed: Polygon is too small for ERA5 Land, trying again with larger polygon")
            dataset = lake_extraction.subset_box(dataset, polygon.buffer(0.5), 0)
        dataset = helpers.fix_weird_units_descriptors(dataset, "e", "m")
        dataset = helpers.add_descriptive_time_component_to_units(dataset, "day")
        dataset = helpers.convert_units(dataset, "mm/month", "tp", "e")
        helpers.pickle_xarray_dataset(dataset)
    else:
        dataset = helpers.load_pickle_dataset("ERA5 Land")
    evap_ds, precip_ds = helpers.spatially_average_dataset(dataset, "e", "tp")
    evap_ds = helpers.make_sure_dataset_is_positive(evap_ds)
    helpers.clean_up_temporary_files()
    list_of_MVSeries = [precip_ds, evap_ds]
    logger.info("ERA Driver Finished")
    return list_of_MVSeries


def CRUTS_driver(polygon, debug=False) -> list[MVSeries]:
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
    logger.info("CRUTS Driver Started: Precip & PET")
    if not debug:
        dataset = CRUTS.get_total_precip_evap()
        dataset = helpers.label_xarray_dataset_with_product_name(dataset, "CRUTS")
        dataset = helpers.fix_lat_long_names(dataset)
        try:
            dataset = lake_extraction.subset_box(dataset, polygon, 1)
        except ValueError:
            logging.error("CRUTS subset Polygon is too small for CRUTS, trying again with larger polygon")
            dataset = lake_extraction.subset_box(dataset, polygon.buffer(0.5), 0)
        dataset = helpers.convert_units(dataset, "mm/month", "pet", "pre")
        helpers.pickle_xarray_dataset(dataset)
    else:
        dataset = helpers.load_pickle_dataset("CRUTS")
    pet_ds, precip_ds = helpers.spatially_average_dataset(dataset, "pet", "pre")
    pet_ds, precip_ds = helpers.move_date_index_to_first_of_the_month(pet_ds, precip_ds)
    helpers.clean_up_temporary_files()
    list_of_MVSeries = [pet_ds, precip_ds]
    logger.info("CRUTS Driver Finished")
    return list_of_MVSeries


if __name__ == "__main__":
    print("Product Driver Functions Accessed Here")
    # CRUTS_driver(lake_extraction.extract_lake(798))

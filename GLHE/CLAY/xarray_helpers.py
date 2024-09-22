import logging

import xarray as xr
import pint_xarray
from GLHE.CLAY.globals import SLC_MAPPING
from GLHE.CLAY.helpers import MVSeries
from . import ureg

logger = logging.getLogger(__name__)


def make_sure_xarray_dataset_is_positive(dataset: xr.Dataset, *vars: str) -> xr.Dataset:
    """
    This function makes sure that the dataset is positive, if not it makes it positive
    Parameters
    ----------
    dataset: xr.Dataset
        the dataset that needs to be converted to positive
    vars: str
        the name of the variable to be converted
    Returns
    -------
    xr.Dataset
        the dataset with all positive values
    """
    for variable_name in vars:
        if dataset[variable_name].values.min() < 0:
            logger.info(
                "The dataset ({} {}) has negative values, making it positive".format(
                    dataset.attrs["product_name"], variable_name
                )
            )
            dataset[variable_name].values = abs(dataset[variable_name].values)
    return dataset


def convert_xarray_dataset_to_mvseries(
    dataset: xr.Dataset, *vars: str
) -> list[MVSeries]:
    """Convert Xarray Dataset

    Parameters
    ----------
    dataset : xr.Dataset
        The daily xarray dataset to group
    vars: str
        The variable names to average over
    Returns
    -------
    tuple of MVSeries
        MVSeries with time & variables
    """
    logger.info(
        "Converted the dataset {} to MVSeries".format(dataset.attrs["product_name"])
    )
    series_list = []
    for var in vars:
        pandas_dataset = dataset.get(var).to_series()
        metadata_series = MVSeries(
            pandas_dataset,
            ureg.parse_expression(dataset.variables[var].attrs["units"]).units,
            SLC_MAPPING.get(var),
            dataset.attrs["product_name"],
            var,
            dataset[var],
        )
        series_list.append(metadata_series)
    return series_list


def spatially_average_xarray_dataset_and_convert(
    dataset: xr.Dataset, *vars: str
) -> tuple[MVSeries]:
    """Spatially average xarray data to one value over entire grid

    Parameters
    ----------
    dataset : xr.Dataset
        The daily xarray dataset to group
    vars: str
        The variable names to average over
    Returns
    -------
    tuple of MVSeries
        MVSeries with time & variables
    """
    lat_name = "lat"
    lon_name = "lon"
    logger.info(
        "Spatially Averaged the dataset {}".format(dataset.attrs["product_name"])
    )
    series_list = []
    for var in vars:
        pandas_dataset = dataset.mean(dim=[lat_name, lon_name]).get(var).to_series()
        metadata_series = MVSeries(
            pandas_dataset,
            ureg.parse_expression(dataset.variables[var].attrs["units"]).units,
            SLC_MAPPING.get(var),
            dataset.attrs["product_name"],
            var,
            dataset[var],
        )
        series_list.append(metadata_series)
    return tuple(series_list)


def fix_lat_long_names_in_xarray_dataset(dataset: xr.Dataset) -> xr.Dataset:
    """
    This function fixes the lat/lon names in the dataset, written by ChatGPT
    Parameters
    ----------
    dataset
        the dataset in question
    Returns
    -------
    dataset
        the dataset with the fixed lat/lon names
    """
    coord_names = list(dataset.coords.keys())

    # Common variations of latitude and longitude names
    lat_variations = ["lat", "latitude"]
    lon_variations = ["lon", "longitude"]

    # Find latitude variable name
    lat_name = next((var for var in coord_names if var in lat_variations), None)

    # Find longitude variable name
    lon_name = next((var for var in coord_names if var in lon_variations), None)

    # Check if latitude and longitude variable names are found
    if lat_name is None or lon_name is None:
        print("Latitude or longitude variable name not found.")
    if lat_name != "lat" or lon_name != "lon":
        dataset = dataset.rename({lat_name: "lat", lon_name: "lon"})
    logger.info(
        "Renamed the dataset {} to lat, long standard names".format(
            dataset.attrs["product_name"]
        )
    )

    return dataset


def rename_xarray_units(
    dataset: xr.Dataset, new_unit: str, *variables: str
) -> xr.Dataset:
    """
    This function renames the units of the dataset, specifically for the pint converter. DONT USE THIS UNLESS YOU REALLY REALLY NEED TO.
    """

    logger.warn("Dont use this unless you understand the function!!")
    for var in variables:
        dataset[var].attrs["units"] = new_unit
    return dataset


def group_xarray_dataset_by_month(dataset: xr.Dataset) -> xr.Dataset:
    """Groups xarray data to monthly values

    Parameters
    ----------
    dataset : xr.Dataset
        The daily xarray dataset to group
    Returns
    -------
    xarray Dataset
        xarray with index of year,month added
    """
    logger.info(
        "Grouped the dataset {} to monthly values".format(dataset.attrs["product_name"])
    )

    return dataset.resample(time="M").sum()


def label_xarray_dataset_with_product_name(
    dataset: xr.Dataset, name: str
) -> xr.Dataset:
    """Adds a label to the dataset describing the product called product_name,
    accessed by dataset.attrs["name"]"""
    logger.info("Adding/Changing Product Name to Dataset to {}".format(name))
    dataset.attrs["product_name"] = name
    return dataset


def fix_weird_units_descriptors_in_xarray_datasets(
    dataset: xr.Dataset, var: str, correct_unit: str
) -> xr.Dataset:
    """
    This function fixes the weird units descriptors in the dataset, like ERA5's 'm of water equivalent' to just 'm'
    Parameters
    ----------
    dataset:xr.Dataset
        the dataset in question
    var:str
        the variable in question
    correct_unit:str
        the correct unit to replace the weird one
    Returns
    -------
    dataset:xr.Dataset
        the dataset with the fixed units
    """
    logger.info(
        "Fixed the weird units descriptors in the dataset {} for variable {} from {} to {}".format(
            dataset.attrs["product_name"],
            var,
            dataset[var].attrs["units"],
            correct_unit,
        )
    )
    dataset[var].attrs["units"] = correct_unit
    return dataset


def add_descriptive_time_component_to_units_in_xarray_dataset(
    dataset: xr.Dataset, time_denominator: str
) -> xr.Dataset:
    """
    This one is a little harder to explain, but for pint_xarray, some of these netcdf files
    don't like do m/month, they just say, m, so this function just adds the /month or /day part.
    Parameters
    ----------
    dataset: xr.Dataset
        the dataset in question
    time_denominator: str
        the time denominator, either month or day
    Returns
    -------
    dataset: xr.Dataset
        the dataset with the time component added to the units
    """
    logger.info(
        "Added the time component ({}) to the units of the dataset {}".format(
            time_denominator, dataset.attrs["product_name"]
        )
    )
    for var in dataset.keys():
        if "units" in dataset[var].attrs and "/" not in dataset[var].attrs["units"]:
            dataset[var].attrs["units"] += f"/{time_denominator}"
    return dataset


def convert_xarray_dataset_units(
    dataset: xr.Dataset, output_unit: str, *variable: str
) -> xr.Dataset:
    """
    Converts the units of the dataset to output_unit, mostly to mm/month or cubic meters/month,
    Parameters
    ----------
    dataset: xr.Dataset
        The input dataset to convert units
    variable: str
        The variable to convert units
    output_unit: str
        The output unit of the dataset
    Returns
    -------
    xr.Dataset
        The dataset with the units converted
    """
    logger.info(
        "Converting the dataset {}, variable {}, to units {}".format(
            dataset.attrs["product_name"], variable, output_unit
        )
    )

    dataset = dataset.pint.quantify(
        {"lat": None, "lon": None, "time": None}, unit_registry=ureg
    )
    for var in variable:
        if dataset[var].pint.units == ureg.parse_units(output_unit):
            continue
        dataset = dataset.pint.to({var: output_unit})
    dataset = dataset.pint.dequantify()
    return dataset

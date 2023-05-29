import os
import xarray as xr
import pandas as pd
import logging

logger = logging.getLogger(__name__)
# Globals

precip_codes = ["p", "precip", "precipitation"]
evap_codes = ["e", "pet", "evap", "evaporation"]
runoff_codes = ["r", "runoff"]


class MVSeries:
    """This class is just to store the pandas series with some extra metadata,
    a dictionary would probably work just as well
    ...

    Attributes
    ----------
    dataset : pd.Series
        a formatted series of the dataset
    product_name : str
        data source
    unit : str
        the unit of the data
    single_letter_code : str
        what kind of product, reference helper globals, precip_codes, evap_codes, & runoff_codes

    Methods
    -------
    set_units(self,unit)
        Changes the unit, does NOT convert it.

    """

    # Class Variables
    dataset: pd.Series
    unit: str
    single_letter_code: str
    product_name: str

    def __init__(self, dataset, unit, single_letter_code, product_name):
        self.dataset = dataset
        self.unit = unit
        self.single_letter_code = single_letter_code
        self.product_name = product_name

    def __str__(self):
        return f"Product: {self.product_name}, SLC: {self.single_letter_code}, Unit: {self.unit}"

    # Change Units MetaData DOESNT CHANGE ACTUAL VALUES
    def set_units(self, unit):
        self.unit = unit


def spatially_average_dataset(dataset: xr.Dataset, var: str) -> pd.Series:
    """Spatially average xarray data to one value over entire grid

        Parameters
        ----------
        dataset : xr.Dataset
            The daily xarray dataset to group
        var: str
            The variable name to average over
        Returns
        -------
        pandas Series
            pandas Series with time & variables
        """
    lat_name = 'lat'
    lon_name = 'lon'
    logger.debug("Spatially Averaged the dataset {}".format(dataset.attrs['name']))

    return dataset.mean(dim=[lat_name, lon_name]).get(var).to_series()


def fix_lat_long_names(dataset: xr.Dataset) -> xr.Dataset:
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
    lat_variations = ['lat', 'latitude']
    lon_variations = ['lon', 'longitude']

    # Find latitude variable name
    lat_name = next((var for var in coord_names if var in lat_variations), None)

    # Find longitude variable name
    lon_name = next((var for var in coord_names if var in lon_variations), None)

    # Check if latitude and longitude variable names are found
    if lat_name is None or lon_name is None:
        print("Latitude or longitude variable name not found.")
    if lat_name != 'lat' or lon_name != 'lon':
        dataset = dataset.rename({lat_name: 'lat', lon_name: 'lon'})
    logger.debug("Renamed the dataset {} to lat, long standard names".format(dataset.attrs["name"]))

    return dataset


def group_dataset_by_month(dataset: xr.Dataset) -> xr.Dataset:
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
    logger.debug("Grouped the dataset {} to monthly values".format(dataset.attrs["name"]))

    return dataset.resample(time='M').sum()


def label_xarray_dataset(dataset: xr.Dataset, name: str) -> xr.Dataset:
    """Adds a label to the dataset describing the product called product_name,
    accessed by dataset.attrs["name"] """

    dataset.attrs["name"] = name
    return dataset


def clean_up_temporary_files() -> list:
    """removes files that start with TEMPORARY

        Parameters
        ----------
        Returns
        -------
        list
            list of filenames of deleted files
        """

    if not os.path.exists(".temp"):
        return []
    deleted_files = []
    for file in os.listdir(".temp"):
        if file.split("_")[0] == "TEMPORARY":
            os.remove(".temp/" + file)
            deleted_files.append(file)
    logger.debug("Cleared temporary files")
    return deleted_files

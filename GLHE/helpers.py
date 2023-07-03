import logging
import os
import pickle
from calendar import monthrange
from dataclasses import dataclass

import pandas as pd
import pint_xarray
import xarray as xr
from pint import Unit

import GLHE.globals
from . import ureg

logger = logging.getLogger(__name__)

# Globals
slc_mapping_reverse = {
    "e": "evap",
    "r": "runoff",
    "p": "precip",
    "o": "outflow"
}
slc_mapping = {
    "e": "e",
    "pet": "e",
    "evap": "e",
    "evaow": "e",
    "p": "p",
    "tp": "p",
    "pre": "p",
    "precip": "p",
    "precipitation": "p",
    "r": "r",
    "runoff": "r",
    "Inflow": "r",
    "Outflow": "o"
    # Add more mappings as needed
}


@dataclass
class MVSeries:
    """This class is just to store the pandas series with some extra metadata,
    a dictionary would probably work just as well, but this is more readable
    spatially_average_dataset function.
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
    variable_name : str
        the name of the variable
    xarray_dataarray : xr.Dataset
        the original xarray dataset - this might be bad

    Methods
    -------

    """

    # Class Variables
    dataset: pd.Series
    unit: Unit
    single_letter_code: str
    product_name: str
    variable_name: str
    xarray_dataarray: xr.DataArray

    def __init__(self, dataset: pd.Series, unit: Unit, single_letter_code: str, product_name: str,
                 variable_name: str, xarr_dataset: xr.DataArray):
        self.dataset = dataset
        self.unit = unit
        self.single_letter_code = single_letter_code
        self.product_name = product_name
        self.variable_name = variable_name
        self.xarray_dataarray = xarr_dataset

    def __str__(self):
        return "Product: {},Variable Name: {} SLC: {}, Unit: {}".format(self.product_name, self.variable_name,
                                                                        self.single_letter_code, self.unit)

    # Change Units MetaData DOESNT CHANGE ACTUAL VALUES
    def set_units(self, unit):
        self.unit = unit


def pickle_xarray_dataset(dataset: xr.Dataset) -> None:
    """Pickle xarray dataset to disk

    Parameters
    ----------
    dataset : xr.Dataset
        The xarray dataset to pickle
    Returns
    -------
    None

    """
    pickle_file = GLHE.globals.OUTPUT_DIRECTORY + "/save_files/" + GLHE.globals.LAKE_NAME + "_" + dataset.attrs[
        'name'] + '.pickle'
    with open(pickle_file, 'wb') as file:
        pickle.dump(dataset, file)


def convert_dicts_to_MVSeries(date_column_name: str, units_key_name: str, product_name_key_name: str, *dicts: dict) -> \
        list[
            MVSeries]:
    """Convert dictionaries to MVSeries

    Parameters
    ----------
    dicts : dict
        The dictionaries to convert, each must have a "date" column
    date_column_name : str
        The name of the date column
    units_key_name : str
        The name of the units string value
    product_name_key_name : str
        The name of the key to the product name
    Returns
    -------
    list of MVSeries
        MVSeries of the data

    """
    series_list = []
    for dictionary in dicts:
        for key, value in dictionary.items():
            if key != date_column_name and key != units_key_name and key != product_name_key_name:
                series = pd.Series(value, index=dictionary[date_column_name])
                metadata_series = MVSeries(series, ureg.parse_expression(dictionary[units_key_name]),
                                           slc_mapping.get(key),
                                           dictionary[product_name_key_name], key, None)
                series_list.append(metadata_series)
    return series_list


def load_pickle_dataset(pickle_file: str) -> xr.Dataset:
    """Load pickled xarray dataset from disk

    Parameters
    ----------
    pickle_file : str
        The xarray dataset to pickle
    Returns
    -------
    xr.Dataset

    """
    with open(GLHE.globals.OUTPUT_DIRECTORY + "/save_files/" + GLHE.globals.LAKE_NAME + "_" + pickle_file + ".pickle",
              'rb') as file:
        dataset = pickle.load(file)
    return dataset


def make_sure_dataset_is_positive(dataset: xr.Dataset, *vars: str) -> xr.Dataset:
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
            logger.info("The dataset ({} {}) has negative values, making it positive".format(dataset.attrs['name'],
                                                                                             variable_name))
            dataset[variable_name].values = abs(dataset[variable_name].values)
    return dataset


def move_date_index_to_first_of_the_month(*series: MVSeries) -> tuple[MVSeries]:
    """
    This function moves the date index to the first of the month
    Parameters
    ----------
    series: MVSeries
        the series to be modified
    Returns
    -------
    list[MVSeries]
        the modified series
    """
    series_list = []
    for s in series:
        s.dataset.index = s.dataset.index - pd.offsets.MonthBegin(1)
        series_list.append(s)
    return tuple(series_list)


def spatially_average_dataset_and_convert(dataset: xr.Dataset, *vars: str) -> tuple[MVSeries]:
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
    lat_name = 'lat'
    lon_name = 'lon'
    logger.info("Spatially Averaged the dataset {}".format(dataset.attrs['name']))
    series_list = []
    for var in vars:
        pandas_dataset = dataset.mean(dim=[lat_name, lon_name]).get(var).to_series()
        metadata_series = MVSeries(pandas_dataset, ureg.parse_expression(dataset.variables[var].attrs['units']).units,
                                   slc_mapping.get(var),
                                   dataset.attrs['name'], var, dataset[var])
        series_list.append(metadata_series)
    return tuple(series_list)


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


def group_MVSeries_by_month(list_of_datasets: list[MVSeries]) -> list[MVSeries]:
    """Groups MVSeries data to monthly values"""

    for mvs in list_of_datasets:
        mvs.dataset = mvs.dataset.groupby(pd.Grouper(freq='MS')).mean()
    return list_of_datasets


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
    logger.debug("Grouped the dataset {} to monthly values".format(dataset.attrs["name"]))

    return dataset.resample(time='M').sum()


def label_xarray_dataset_with_product_name(dataset: xr.Dataset, name: str) -> xr.Dataset:
    """Adds a label to the dataset describing the product called product_name,
    accessed by dataset.attrs["name"] """

    dataset.attrs["name"] = name
    return dataset


def fix_weird_units_descriptors(dataset: xr.Dataset, var: str, correct_unit: str) -> xr.Dataset:
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
    logger.info("Fixed the weird units descriptors in the dataset {} for variable {} from {} to {}".format(
        dataset.attrs["name"], var, dataset[var].attrs["units"], correct_unit))
    dataset[var].attrs["units"] = correct_unit
    return dataset


def add_descriptive_time_component_to_units(dataset: xr.Dataset, time_denominator: str) -> xr.Dataset:
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
        "Added the time component ({}) to the units of the dataset {}".format(time_denominator, dataset.attrs["name"]))
    for var in dataset.keys():
        if "units" in dataset[var].attrs and '/' not in dataset[var].attrs["units"]:
            dataset[var].attrs["units"] += f'/{time_denominator}'
    return dataset


def convert_xarray_units(dataset: xr.Dataset, output_unit: str, *variable: str) -> xr.Dataset:
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
        "Converted the dataset {}, variable {}, to units {}".format(dataset.attrs["name"], variable, output_unit))

    dataset = dataset.pint.quantify({"lat": None, "lon": None})
    for var in variable:
        if dataset[var].pint.units == pint_xarray.unit_registry.parse_units(output_unit):
            continue
        dataset = dataset.pint.to({var: output_unit})
    dataset = dataset.pint.dequantify()
    return dataset


def convert_MVSeries_units(list_of_MVSeries, output_unit: Unit) -> list[MVSeries]:
    """
    Converts the units of the MVSeries to output_unit, mostly to mm/month or cubic meters/month,
    Parameters
    ----------
    list_of_MVSeries: list[MVSeries]
        The input list of MVSeries to convert units
    output_unit: pint.Unit
        The output unit of the dataset
    Returns
    -------
    list[MVSeries]
        The list of MVSeries with the units converted
    """
    for var in list_of_MVSeries:
        string_input_units = str(var.unit)
        string_output_units = str(output_unit)
        if '/' in string_input_units:
            if not '/' in string_output_units:
                raise ValueError(
                    "The output unit {} is not a rate, but the input unit {} is a rate".format(output_unit, var.unit))
            else:
                numer_input_unit, denom_input_unit = str(string_input_units).split('/')
                numer_output_unit, denom_output_unit = str(string_output_units).split('/')
                denom_output_unit.replace(' ', '')
                denom_input_unit.replace(' ', '')
                numer_output_unit.replace(' ', '')
                numer_input_unit.replace(' ', '')
                conversion_factor_numerator = ureg.Quantity(1, numer_input_unit).to(numer_output_unit).magnitude
                if denom_output_unit == 'month':
                    for i in range(len(var.dataset)):
                        days_in_month = monthrange(var.dataset.index[i].year, var.dataset.index[i].month)[1]
                        conversion_factor_to_days = ureg.Quantity(1, var.unit).to(
                            ureg(numer_output_unit + '/days')).magnitude
                        var.dataset[i] *= conversion_factor_to_days * days_in_month
                else:
                    conversion_factor = ureg.Quantity(1, var.unit).to(output_unit).magnitude
                    var.dataset *= conversion_factor
                var.unit = output_unit
        else:
            conversion_factor = ureg.Quantity(1, string_input_units).to(string_output_units).magnitude
            var.dataset *= conversion_factor
            var.unit = output_unit
    return list_of_MVSeries


def setup_output_directory(output_directory: str) -> None:
    """Makes the output directory if it doesn't exist

    Parameters
    ----------
    output_directory : str
        The output directory to make
    """
    if not os.path.exists(output_directory.replace('\\', '')):
        os.mkdir(output_directory)
        os.mkdir(output_directory + "/save_files")
        logger.info("Made the output directory {}".format(output_directory))
    GLHE.globals.OUTPUT_DIRECTORY = os.getcwd() + "/" + output_directory


def clean_up_all_temporary_files() -> list:
    """removes files that start with TEMPORARY

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


def clean_up_specific_temporary_files(key: str) -> list:
    """removes files that start with TEMPORARY
        Parameters
        ----------
        key:str
            the key to remove
        Returns
        -------
        list
            list of filenames of deleted files
        """

    if not os.path.exists(".temp"):
        return []
    deleted_files = []
    for file in os.listdir(".temp"):
        if file.split("_")[0] == "TEMPORARY" and file.split("_")[1] == key:
            os.remove(".temp/" + file)
            deleted_files.append(file)
    logger.debug("Cleared temporary  with key {} in their filename after the understore".format(key))
    return deleted_files

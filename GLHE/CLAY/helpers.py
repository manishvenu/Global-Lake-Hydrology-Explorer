import logging
import os
import pickle
from calendar import monthrange
from dataclasses import dataclass

import pandas as pd
import xarray as xr
from pint import Unit
import GLHE.CLAY.globals
from GLHE.CLAY import ureg

logger = logging.getLogger(__name__)


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
        what kind of product, reference helper GLHE.CLAY.globals, precip_codes, evap_codes, & runoff_codes
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
        if "_" in product_name:
            product_name = str.replace(product_name, "_", "-")
        self.product_name = product_name

        self.variable_name = variable_name
        self.xarray_dataarray = xarr_dataset

    def __str__(self):
        return "Product: {},Variable Name: {} SLC: {}, Unit: {}".format(self.product_name, self.variable_name,
                                                                        self.single_letter_code, self.unit)

    # Change Units MetaData DOESNT CHANGE ACTUAL VALUES
    def set_units(self, unit):
        self.unit = unit


def pickle_var(variable: object, identification_string: str) -> None:
    """
    Pickle a variable to disk
    """
    if GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] is None or GLHE.CLAY.globals.config["DIRECTORIES"][
        "OUTPUT_DIRECTORY"] == "":
        raise Exception(
            "GLHE.CLAY.globals.config[\"DIRECTORIES\"][\"OUTPUT_DIRECTORY\"] is not set, need to have a directory to place pickle files")
    pickle_file_name = GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/save_files/" + \
                       GLHE.CLAY.globals.config[
                           "LAKE_NAME"] + "_" + identification_string + '.pickle'
    with open(pickle_file_name, 'wb') as file:
        pickle.dump(variable, file)


def unpickle_var(identification_string: str) -> object:
    """
    Load a variable from disk, used in conjuction with pickle_var
    """
    pickle_file_name = GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/save_files/" + \
                       GLHE.CLAY.globals.config[
                           "LAKE_NAME"] + "_" + identification_string + '.pickle'
    with open(pickle_file_name, 'rb') as file:
        variable = pickle.load(file)
    return variable


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
                                           GLHE.CLAY.globals.SLC_MAPPING.get(key),
                                           dictionary[product_name_key_name], key, None)
                series_list.append(metadata_series)
    return series_list


def move_date_index_to_first_of_the_month(*series: MVSeries) -> list[MVSeries]:
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
    for s in list(series):
        s.dataset.index = s.dataset.index - pd.offsets.MonthBegin(1)
        series_list.append(s)
    return series_list


def group_MVSeries_by_month(list_of_datasets: list[MVSeries]) -> list[MVSeries]:
    """Groups MVSeries data to monthly values"""
    logger.info("Grouping MVSeries by month")
    for mvs in list_of_datasets:
        mvs.dataset = mvs.dataset.groupby(pd.Grouper(freq='MS')).mean()
    return list_of_datasets


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


def setup_output_directory(lake_name: str) -> None:
    """Makes the output directory if it doesn't exist

    Parameters
    ----------
    lake_name : str
        The output directory to make
    """
    if not os.path.exists(GLHE.CLAY.globals.config["DIRECTORIES"]["LAKE_OUTPUT_FOLDER"]):
        os.mkdir(GLHE.CLAY.globals.config["DIRECTORIES"]["LAKE_OUTPUT_FOLDER"])
    full_filepath = os.path.join(GLHE.CLAY.globals.config["DIRECTORIES"]["LAKE_OUTPUT_FOLDER"], lake_name)
    if not os.path.exists(full_filepath):
        os.mkdir(full_filepath)
        os.mkdir(full_filepath + "/save_files")
        logger.info("Made the output directory {}".format(full_filepath))
    GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] = full_filepath
    GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY_SAVE_FILES"] = os.path.join(full_filepath, "/save_files")
    if GLHE.CLAY.globals.config["LAKE_NAME"] == None:
        logger.warning("There is no lake name set! Don't forget to set that!")


def setup_logging_directory(folder: str) -> None:
    """Makes the logging directory if it doesn't exist
    """
    lake_folder_logging_directory = os.path.join(folder, "logging")
    if GLHE.CLAY.globals.config["DIRECTORIES"]["LOGGING_DIRECTORY"] is not None and os.path.exists(
            lake_folder_logging_directory):
        logger.info("A logging directory already exists")
    if not os.path.exists(folder):
        os.mkdir(folder)
    if not os.path.exists(lake_folder_logging_directory):
        os.mkdir(lake_folder_logging_directory)
        GLHE.CLAY.globals.config["DIRECTORIES"]["LOGGING_DIRECTORY"] = lake_folder_logging_directory
        logger.info(
            "Created logging directory of the new folder requested, and repointed the logging directory. "
            "Newly created log file handlers will point here instead.")
    elif GLHE.CLAY.globals.config["DIRECTORIES"]["LOGGING_DIRECTORY"] != lake_folder_logging_directory:
        GLHE.CLAY.globals.config["DIRECTORIES"]["LOGGING_DIRECTORY"] = lake_folder_logging_directory
        logger.info("Filled in logging directory pointer to folder that already existed")


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
    """removes files that start with TEMPORARY_[key]
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
    logger.debug("Cleared temporary  with key {} in their filename after the underscore".format(key))
    return deleted_files

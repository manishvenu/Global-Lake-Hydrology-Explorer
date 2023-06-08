import os
import xarray as xr
import logging

logger = logging.getLogger(__name__)


def check_if_module_works() -> str:
    """A simple check if user is able to access functions in this file"""

    return "Success"


def get_total_precip_evap() -> xr.Dataset:
    """Gets CRUTS evap

    Parameters
    ----------
    None

    Returns
    -------
    xarray Dataset
        xarray Dataset format of the evap, precip, & runoff in a grid
    """
    logger.info("Reading in CRUTS data from LocalData folder")
    ds = xr.load_dataset("LocalData\\cruts_pet_pre_4.07_1901_2022.nc")
    return ds


if __name__ == "__main__":
    print("This is the CRUTS module, not a script")

import xarray as xr
import logging
logger = logging.getLogger(__name__)

def check_if_module_works() -> str:
    """A simple check if user is able to access functions in this file"""

    return "Success"

def get_total_evap_in_sub_region(hyset_id: str) -> xr.Dataset:
    """Gets GLEV evap in a known subregion by lat-long

    Parameters
    ----------
    hyset_id : str
        HYSET Lake ID

    Returns
    -------
    xarray Dataset
        xarray Dataset format of the evap, precip, & runoff in a grid
    """

    return None
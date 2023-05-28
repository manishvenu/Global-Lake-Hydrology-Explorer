import os

import xarray as xr
import pandas as pd


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
    coordinate_names = list(dataset.coords.keys())
    lat_name = 'lat'
    lon_name = 'lon'
    return dataset.mean(dim=[lat_name, lon_name]).get(var).to_series()

def fix_lat_long_names(dataset:xr.Dataset) -> xr.Dataset:
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
    if lat_name is not None and lon_name is not None:
        print(f"Latitude variable name: {lat_name}")
        print(f"Longitude variable name: {lon_name}")
    else:
        print("Latitude or longitude variable name not found.")
    dataset = dataset.rename({lat_name: 'lat', lon_name: 'lon'})

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

    return dataset.resample(time='M').sum()


def clean_up_temporary_files() -> list:
    """removes files that start with TEMPORARY

        Parameters
        ----------
        None
        Returns
        -------
        list
            list of filenames of deleted files
        """

    if (not os.path.exists(".temp")):
        return []
    deleted_files = []
    for file in os.listdir(".temp"):
        if file.split("_")[0] == "TEMPORARY":
            os.remove(".temp/"+file)
            deleted_files.append(file)
    return deleted_files

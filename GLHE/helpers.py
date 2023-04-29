import xarray as xr
import pandas as pd
def spatially_average_dataset(dataset:xr.Dataset,var:str)->pd.Series:
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
    lat_name='lat'
    lon_name = 'lon'
    return dataset.mean(dim=[lat_name,lon_name]).get(var).to_series()
def group_dataset_by_month(dataset:xr.Dataset)->xr.Dataset:
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
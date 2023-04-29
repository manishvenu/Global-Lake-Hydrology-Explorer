from GLHE.data_access import ERA5_Land
from GLHE import helpers
import xarray as xr
import matplotlib.pyplot as plt

def plot(dataset:xr.Dataset)->None:
    """Plot Precip, Evap, & Runoff in a three panel plot

        Parameters
        ----------
        dataset : xr.Dataset
            The monthly xarray dataset
        Returns
        -------
        None
        """
    print("Plotting done here")

def output_compiled_data_to_csv(dataset:xr.Dataset,filename:str)->None:
    """Output data into csv

        Parameters
        ----------
        dataset : xr.Dataset
            The monthly xarray dataset
        filename: str
            The output file name/location
        Returns
        -------
        a csv file in the location specified
        """
    print("Output CSV accessed here")
def main():
    era5 = ERA5_Land.get_total_precip_runoff_evap_in_sub_region(0.1,0.2,0.1,0.2)
    era5 = helpers.spatially_average_dataset(era5)
    print(era5)
if __name__ == "__main__":
    main()
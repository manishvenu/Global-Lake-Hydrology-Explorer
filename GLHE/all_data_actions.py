from GLHE import helpers, lake_extraction,data_access
import xarray as xr
import matplotlib.pyplot as plt

def plot_all_data(dataset:xr.Dataset)->None:
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

def output_all_compiled_data_to_csv(dataset:xr.Dataset,filename:str)->None:
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
def driver(hyset_id:int)->None:
    """
    This is the driver function for the all_data_actions.py file.
    Here, we'll access all the data
    """
    

    x=1

if __name__ == "__main__":
    driver(798)
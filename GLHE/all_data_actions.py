from GLHE import helpers, lake_extraction, data_access, product_driver_functions
from GLHE.helpers import MVSeries
import xarray as xr
import matplotlib.pyplot as plt
import pandas as pd


def plot_all_data(dataset: pd.DataFrame) -> None:
    """Plot Precip, Evap, & Runoff in a three panel plot

        Parameters
        ----------
        dataset : pd.DataFrame
            The monthly Pandas dataset
        Returns
        -------
        None
        """
    print("Plotting done here")


def output_all_compiled_data_to_csv(dataset: pd.DataFrame, filename: str) -> None:
    """Output data into csv

        Parameters
        ----------
        dataset : pd.DataFrame
            The monthly pandas Dataframe dataset
        filename: str
            The output file name/location
        Returns
        -------
        a csv file in the location specified
        """
    print("Output CSV accessed here:", ".temp/"+filename)
    dataset.to_csv(".temp/"+filename)


def merge_mv_series(*datasets: MVSeries) -> pd.DataFrame:
    """
    Merges mv_series from all the products into a single dataframe
    Parameters
    ----------
    datasets : list[mv_series]
        List of mv_series to be merged
    Returns
    -------
    pd.DataFrame
        Merged mv_series
    """
    col_names = []
    precip = []
    evap = []
    runoff = []
    for i in datasets:
        if i.single_letter_code == "p":
            precip.append(i.dataset)
        elif i.single_letter_code == "e" or i.single_letter_code == "pet":
            evap.append(i.dataset)
        elif i.single_letter_code == "r":
            runoff.append(i.dataset)
        col_names.append(i.single_letter_code + "." + i.product_name)

    df = pd.concat(precip + evap + runoff, axis=1).set_axis(labels=col_names, axis=1)
    df.index.name = "Date"
    return df


if __name__ == "__main__":
    print("This is the all_data_actions file")

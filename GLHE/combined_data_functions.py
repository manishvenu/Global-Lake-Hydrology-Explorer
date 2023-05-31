from GLHE import lake_extraction, data_access, product_driver_functions
from GLHE.helpers import *
import xarray as xr
import matplotlib.pyplot as plt
import pandas as pd
import logging

logger = logging.getLogger(__name__)


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
    logger.info("Plotting Data")
    precip_cols = [col for col in dataset if col.startswith("p")]
    evap_cols = [col for col in dataset if col.startswith("e")]
    runoff_cols = [col for col in dataset if col.startswith("r")]

    fig, axs = plt.subplots(3, 1, figsize=(10, 10))
    dataset.plot(y=precip_cols, ax=axs[0],
                 xlim=(dataset[precip_cols].first_valid_index(), dataset[precip_cols].last_valid_index()))
    dataset.plot(y=evap_cols, ax=axs[1],
                 xlim=(dataset[evap_cols].first_valid_index(), dataset[evap_cols].last_valid_index()))
    if (len(runoff_cols) > 0):
        dataset.plot(y=runoff_cols, ax=axs[2],
                     xlim=(dataset[runoff_cols].first_valid_index(), dataset[runoff_cols].last_valid_index()))
    plt.tight_layout()
    plt.show()
    return


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
    logger.info("Output CSV written here: .temp/" + filename)
    dataset.to_csv(".temp/" + filename)


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
    precip_col_names = []
    evap_col_names = []
    runoff_col_names = []
    precip = []
    evap = []
    runoff = []
    for i in datasets:
        if i.single_letter_code == "p":
            precip.append(i.dataset)
            precip_col_names.append(i.single_letter_code + "." + i.product_name)
        elif i.single_letter_code == "e" or i.single_letter_code == "pet":
            evap.append(i.dataset)
            evap_col_names.append(i.single_letter_code + "." + i.product_name)
        elif i.single_letter_code == "r":
            runoff.append(i.dataset)
            runoff_col_names.append(i.single_letter_code + "." + i.product_name)

    df = pd.concat(precip + evap + runoff, axis=1).set_axis(labels=precip_col_names + evap_col_names + runoff_col_names,
                                                            axis=1)
    df.index.name = "date"
    logger.info("Merged datasets into pandas dataframe")

    return df


if __name__ == "__main__":
    print("This is the all_data_actions file")

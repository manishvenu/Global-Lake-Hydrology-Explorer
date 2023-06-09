import zipfile

import matplotlib.pyplot as plt
import pandas as pd
import rasterio

import GLHE.globals
from GLHE.helpers import *

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
    if len(runoff_cols) > 0:
        dataset.plot(y=runoff_cols, ax=axs[2],
                     xlim=(dataset[runoff_cols].first_valid_index(), dataset[runoff_cols].last_valid_index()))
    plt.tight_layout()
    plt.show()
    plt.save_fig(GLHE.globals.OUTPUT_DIRECTORY + "/" + GLHE.globals.LAKE_NAME+"_products_plot.png")
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
    logger.info("Output CSV written here: " + GLHE.globals.OUTPUT_DIRECTORY + "/" + filename)
    dataset.to_csv(GLHE.globals.OUTPUT_DIRECTORY + "/" + filename)


def merge_mv_series_into_pandas_dataframe(*datasets: MVSeries) -> pd.DataFrame:
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

    df = pd.concat(precip + evap, axis=1).set_axis(labels=precip_col_names + evap_col_names,
                                                   axis=1)
    df.index.name = "date"
    logger.info("Merged datasets into pandas dataframe")

    return df


def present_mv_series_as_geospatial_at_date_time(date: pd.Timestamp, *dss: MVSeries) -> None:
    """
    present xarray datasets into some format, partially written by ChatGPT
    Parameters
    ----------
    datasets : list[mv_series]
        List of mv_series to be merged
    date : pd.Timestamp
        The date to be presented
    filename : str
        The output file name/location
    """
    logger.info("Outputting datasets on {} to GeoTIFF".format(date.strftime('%Y%m%d')))
    input_files = []
    input_filenames = []
    for counter, mvs in enumerate(dss):
        variable = mvs.xarray_dataarray
        lat = variable['lat'].values
        lon = variable['lon'].values
        filename = mvs.variable_name + "_" + mvs.product_name + "_" + GLHE.globals.LAKE_NAME + "_" + date.strftime(
            '%Y%m%d') + ".tif"
        filepath = ".temp/TEMPORARY_" + filename
        input_files.append(filepath)
        input_filenames.append(filename)
        width = len(lon)
        height = len(lat)
        nearest_idx = list(variable.time.values).index(variable.sel(time=date, method='nearest').time)

        # Create the raster layer using rasterio
        with rasterio.open(filepath, 'w', driver='GTiff', height=len(lat), width=len(lon), count=1,
                           dtype=variable.dtype, crs='EPSG:4326',
                           transform=rasterio.transform.from_bounds(lon.min(), lat.min(), lon.max(), lat.max(), width,
                                                                    height)) as dst:
            dst.write(variable[nearest_idx], 1)

    zip_file = GLHE.globals.OUTPUT_DIRECTORY + "/" + GLHE.globals.LAKE_NAME + "_data_layers_on" + date.strftime('%Y%m%d') + ".zip"

    # Create a new ZIP file
    with zipfile.ZipFile(zip_file, 'w') as zf:
        # Add each GeoTIFF file to the ZIP archive
        for index, file in enumerate(input_files):
            zf.write(file, input_filenames[index])

    print(f"GeoTIFF files zipped successfully: {zip_file}")


if __name__ == "__main__":
    print("This is the all_data_actions file")

import zipfile

import matplotlib.pyplot as plt
import pandas as pd
import rasterio

import GLHE.globals
from GLHE.helpers import *
from GLHE.globals import SLC_MAPPING, SLC_MAPPING_REVERSE_UNITS, SLC_MAPPING_REVERSE_NAMES
import snakemd

logger = logging.getLogger(__name__)


def merge_mv_series_into_pandas_dataframe(datasets_dict: dict) -> pd.DataFrame:
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
    df = pd.DataFrame()
    for key in SLC_MAPPING_REVERSE_NAMES:
        for ds in datasets_dict[key]:
            col_name = ds.single_letter_code + "." + ds.product_name
            ds.dataset.name = col_name
            df = pd.merge(df, ds.dataset.to_frame(), how='outer', left_index=True, right_index=True)
    logger.info("Merged datasets into pandas dataframe")
    return df


def output_plot_of_all_data(dataset: pd.DataFrame) -> None:
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
    plotting_dict = {}
    for key in SLC_MAPPING_REVERSE_NAMES:
        temp = [col for col in dataset if col.startswith(key)]
        if len(temp) > 0:
            plotting_dict[key] = temp

    fig, axs = plt.subplots(len(plotting_dict), 1, figsize=(10, 10))
    for index, key in enumerate(plotting_dict):
        dataset.plot(y=plotting_dict[key], ax=axs[index],
                     xlim=(
                         dataset[plotting_dict[key]].first_valid_index(),
                         dataset[plotting_dict[key]].last_valid_index()))
        axs[index].set_ylabel(SLC_MAPPING_REVERSE_NAMES[key] + " " + SLC_MAPPING_REVERSE_UNITS[key])
    plt.tight_layout()
    plt.savefig(GLHE.globals.OUTPUT_DIRECTORY + "/" + GLHE.globals.LAKE_NAME + "_products_plot.png")
    return


def output_all_compiled_data_to_csv(filename: str, dataset: pd.DataFrame) -> None:
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
        filepath = ".temp/TEMPORARY_GEOTIFF_" + filename
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

    zip_file = GLHE.globals.OUTPUT_DIRECTORY + "/" + GLHE.globals.LAKE_NAME + "_gridded_data_layers_on_" + date.strftime(
        '%Y%m%d') + ".zip"

    # Create a new ZIP file
    with zipfile.ZipFile(zip_file, 'w') as zf:
        # Add each GeoTIFF file to the ZIP archive
        for index, file in enumerate(input_files):
            zf.write(file, input_filenames[index])

    logger.info(f"GeoTIFF files zipped successfully: {zip_file}")
    clean_up_specific_temporary_files("GEOTIFF")


def write_and_output_README(data_products: dict) -> None:
    """
    Create a README file that explains all exported data.
    It's hardcoded!! How do we figure out a way to send an information out from functions...frick. I hate that this is a full circle movement. (Event Bus/Observer)
    """
    READ_ME_Name = GLHE.globals.OUTPUT_DIRECTORY + "/" + GLHE.globals.LAKE_NAME + "_README"
    doc = snakemd.new_doc()
    doc.add_heading(GLHE.globals.LAKE_NAME + " README Information!")
    doc.add_paragraph("The following data products have been used and have the following important information:")

    list_of_data_products_readme = []
    for key in data_products:
        list_of_data_products_readme.append(key + ": " + data_products[key]["README"])
    doc.add_unordered_list(list_of_data_products_readme)
    doc.add_paragraph("There are severeal files in this repo, here is the list of files:")
    Items = [
        snakemd.Inline("**" + "save_files/" + "**: " + "Files used by the program to save you time on reruns!"),
        snakemd.Inline("**" + "logging/" + "**: " + "Logging files stating what the program did!"),
        snakemd.Inline("**" + GLHE.globals.LAKE_NAME + "_README.md" + "**: " + "This file!"),
        snakemd.Inline("**" +
                       GLHE.globals.LAKE_NAME + "_Data.csv" + "**: " + "The main output of this program holding a time series of all data!"),
        snakemd.Inline("**" + GLHE.globals.LAKE_NAME + "_products_plot.png" + "**: " + "A plot of the data!"),
        snakemd.Inline("**" +
                       GLHE.globals.LAKE_NAME + "_gridded_Data_layers_on_yyymmdd.zip" + "**: " + "A zip file with files openable in a GIS software telling you what grids were taken for this lake. This is useful when you want to validate what data was taken to create estimates for the gridded products  (ERA5, CRUTS...)."),
        snakemd.Inline("**" +
                       GLHE.globals.LAKE_NAME + "_NWM_Verification_Point_Shapefile/" + "**: " + "A center point chosen by the program and the NWM as the lake in queston. It's worth checking out to make sure the NWM took the right lake."),
    ]
    doc.add_ordered_list(Items)
    doc.dump(READ_ME_Name)
    logger.info("Wrote README file to: " + READ_ME_Name)
    return


if __name__ == "__main__":
    print("This is the all_data_actions file")

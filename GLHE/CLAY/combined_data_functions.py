import zipfile
import logging
import matplotlib.pyplot as plt
import pandas as pd
import rasterio
import snakemd
from pubsub import pub
import json
from GLHE.CLAY.globals import SLC_MAPPING_REVERSE_UNITS, SLC_MAPPING_REVERSE_NAMES
from GLHE.CLAY import events
from GLHE.CLAY.helpers import *

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
    if len(plotting_dict) == 0:
        logger.info("No data to plot")
        return
    fig, axs = plt.subplots(len(plotting_dict), 1, figsize=(10, 10))
    for index, key in enumerate(plotting_dict):
        dataset.plot(y=plotting_dict[key], ax=axs[index],
                     xlim=(
                         dataset[plotting_dict[key]].first_valid_index(),
                         dataset[plotting_dict[key]].last_valid_index()))
        axs[index].set_ylabel(SLC_MAPPING_REVERSE_NAMES[key] + " " + SLC_MAPPING_REVERSE_UNITS[key])
    plt.tight_layout()
    plt.savefig(
        GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/" + GLHE.CLAY.globals.config[
            "LAKE_NAME"] + "_products_plot.png")
    pub.sendMessage(events.topics["output_file_event"],
                    message=events.OutputFileEvent(GLHE.CLAY.globals.config["LAKE_NAME"] + "_products_plot.png",
                                                   GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/" +
                                                   GLHE.CLAY.globals.config["LAKE_NAME"] + "_products_plot.png",
                                                   ".png",
                                                   "A plot of all data, a simple initial visualization of the data",
                                                   events.TypeOfFileLIME.OTHER))

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
    logger.info(
        "Output CSV written here: " + GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/" + filename)
    pub.sendMessage(events.topics["output_file_event"],
                    message=events.OutputFileEvent(filename,
                                                   GLHE.CLAY.globals.config["DIRECTORIES"][
                                                       "OUTPUT_DIRECTORY"] + "/" + filename,
                                                   ".csv",
                                                   "A csv file of all data, the main product",
                                                   events.TypeOfFileLIME.SERIES_DATA))

    dataset.to_csv(GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/" + filename)


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
        filename = mvs.single_letter_code + "_" + mvs.product_name + "_" + GLHE.CLAY.globals.config[
            "LAKE_NAME"] + "_" + date.strftime(
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

    zip_file = GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/" + GLHE.CLAY.globals.config[
        "LAKE_NAME"] + "_gridded_data_layers_on_" + date.strftime(
        '%Y%m%d') + ".zip"
    pub.sendMessage(events.topics["output_file_event"], message=events.OutputFileEvent(zip_file, zip_file, ".zip",
                                                                                       "A zip file of GIS information on gridded products",
                                                                                       events.TypeOfFileLIME.GRIDDED_DATA_FOLDER))
    # Create a new ZIP file
    with zipfile.ZipFile(zip_file, 'w') as zf:
        # Add each GeoTIFF file to the ZIP archive
        for index, file in enumerate(input_files):
            zf.write(file, input_filenames[index])

    logger.info(f"GeoTIFF files zipped successfully: {zip_file}")
    clean_up_specific_temporary_files("GEOTIFF")


def write_and_output_README(read_me_information: dict) -> None:
    """
    Create a README file that explains all exported data in human-readable format
    It's hardcoded!! How do we figure out a way to send an information out from functions...frick. I hate that this is a full circle movement. (Event Bus/Observer)
    """
    READ_ME_Name = GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/" + GLHE.CLAY.globals.config[
        "LAKE_NAME"] + "_README"
    doc = snakemd.new_doc()
    doc.add_heading(GLHE.CLAY.globals.config["LAKE_NAME"] + " README Information!")
    doc.add_paragraph("The following data products have been used and have the following important information:")

    list_of_data_products_readme = []
    for key in read_me_information["Data_Product"]:
        list_of_data_products_readme.append(key + ": " + read_me_information["Data_Product"][key])
    doc.add_unordered_list(list_of_data_products_readme)
    doc.add_paragraph("There are severeal files in this repo, here is the list of files:")
    Items = []
    for key in read_me_information["Output_File"]:
        Items.append(snakemd.Inline("**" + key + "**: " + read_me_information["Output_File"][key]))
    doc.add_ordered_list(Items)
    doc.dump(READ_ME_Name)
    logger.info("Wrote README file to: " + READ_ME_Name)
    pub.sendMessage(events.topics["output_file_event"],
                    message=events.OutputFileEvent(READ_ME_Name,
                                                   GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/" +
                                                   READ_ME_Name,
                                                   ".md",
                                                   "README FIle", events.TypeOfFileLIME.READ_ME))
    return


def write_and_output_LIME_CONFIG(config_information: dict) -> None:
    """
    Create a CONFIG file that points to all exported data in LIME-readable format
    It's hardcoded!! How do we figure out a way to send an information out from functions...frick. I hate that this is a full circle movement. (Event Bus/Observer)
    """
    CONFIG_Name = GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/CONFIG.json"
    ribbit = {"README": config_information[events.TypeOfFileLIME.READ_ME],
              "BLEEPBLEEP": config_information[events.TypeOfFileLIME.BLEEPBLEEP],
              "OTHER": config_information[events.TypeOfFileLIME.OTHER],
              "SERIES_DATA": config_information[events.TypeOfFileLIME.SERIES_DATA],
              "GRIDDED_DATA_FOLDER": config_information[events.TypeOfFileLIME.GRIDDED_DATA_FOLDER],
              "CLAY_OUTPUT_FOLDER_LOCATION": GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"],
              "LAKE_NAME": GLHE.CLAY.globals.config["LAKE_NAME"]}
    try:
        ribbit["NWM_LAKE_POINT_SHAPEFILENAME"] = config_information[events.TypeOfFileLIME.NWM_LAKE_POINT_SHAPEFILENAME]
    except:
        logger.info("No NWM PubSub Event")

    with open(CONFIG_Name, 'w') as fp:
        json.dump(ribbit, fp)
    config_pointer_file_name = r"C:\Users\manis\OneDrive - Umich\Documents\Global Lake Hydrology Explorer\GLHE\LIME\config\config.json"
    with open(config_pointer_file_name, 'r') as f:
        config_pointer = json.load(f)
    config_pointer["CLAY_OUTPUT_FOLDER_LOCATION"] = GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
    with open(config_pointer_file_name, 'w') as fp:
        json.dump(config_pointer, fp)
    logger.info("Wrote CONFIG file to: " + CONFIG_Name)
    return


if __name__ == "__main__":
    print("This is the all_data_actions file")

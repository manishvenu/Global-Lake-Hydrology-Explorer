import zipfile
import logging
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
import snakemd
import json
import GLHE.CLAY.globals
from GLHE.CLAY.globals import SLC_MAPPING_REVERSE_UNITS, SLC_MAPPING_REVERSE_NAMES
from GLHE.CLAY.helpers import MVSeries, clean_up_specific_temporary_files
from GLHE.CALCITE import pubsub, events

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
            df = pd.merge(
                df,
                ds.dataset.to_frame(),
                how="outer",
                left_index=True,
                right_index=True,
            )
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
        dataset.plot(
            y=plotting_dict[key],
            ax=axs[index],
            xlim=(
                dataset[plotting_dict[key]].first_valid_index(),
                dataset[plotting_dict[key]].last_valid_index(),
            ),
        )
        axs[index].set_ylabel(
            SLC_MAPPING_REVERSE_NAMES[key] + " " + SLC_MAPPING_REVERSE_UNITS[key]
        )
    plt.tight_layout()
    plt.savefig(
        GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
        + "/"
        + GLHE.CLAY.globals.config["LAKE_NAME"]
        + "_products_plot.png"
    )
    pubsub.EventBus.Publish(
        pubsub.EventBus,
        events.OutputFileEvent(
            GLHE.CLAY.globals.config["LAKE_NAME"] + "_products_plot.png",
            GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
            + "/"
            + GLHE.CLAY.globals.config["LAKE_NAME"]
            + "_products_plot.png",
            ".png",
            "A plot of all data, a simple initial visualization of the data",
            events.TypeOfFileLIME.OTHER,
        ),
    )

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
        "Output CSV written here: "
        + GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
        + "/"
        + filename
    )
    pubsub.EventBus.Publish(
        pubsub.EventBus,
        events.OutputFileEvent(
            filename,
            GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
            + "/"
            + filename,
            ".csv",
            "A csv file of all data, the main product",
            events.TypeOfFileLIME.SERIES_DATA,
        ),
    )

    dataset.to_csv(
        GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/" + filename
    )


def present_mv_series_as_geospatial_at_date_time(
    date: pd.Timestamp, *dss: MVSeries
) -> None:
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
    logger.info("Outputting datasets on {} to GeoTIFF".format(date.strftime("%Y%m%d")))
    input_files = []
    input_filenames = []
    for counter, mvs in enumerate(dss):
        variable = mvs.xarray_dataarray
        lat = variable["lat"].values
        lon = variable["lon"].values
        filename = (
            mvs.single_letter_code
            + "_"
            + mvs.product_name
            + "_"
            + GLHE.CLAY.globals.config["LAKE_NAME"]
            + "_"
            + date.strftime("%Y%m%d")
            + ".tif"
        )
        filepath = ".temp/TEMPORARY_GEOTIFF_" + filename
        input_files.append(filepath)
        input_filenames.append(filename)
        width = len(lon)
        height = len(lat)

        try:
            nearest_idx = list(variable.time.values).index(
                variable.sel(time=date, method="nearest").time
            )
        except:
            nearest_idx = list(variable.valid_time.values).index(
                variable.sel(valid_time=date, method="nearest").valid_time
            )

        # Create the raster layer using rasterio
        with rasterio.open(
            filepath,
            "w",
            driver="GTiff",
            height=len(lat),
            width=len(lon),
            count=1,
            dtype=variable.dtype,
            crs="EPSG:4326",
            transform=rasterio.transform.from_bounds(
                lon.min(), lat.min(), lon.max(), lat.max(), width, height
            ),
        ) as dst:
            dst.write(variable[nearest_idx], 1)

    zip_file = (
        GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
        + "/"
        + GLHE.CLAY.globals.config["LAKE_NAME"]
        + "_gridded_data_layers_on_"
        + date.strftime("%Y%m%d")
        + ".zip"
    )
    pubsub.EventBus.Publish(
        pubsub.EventBus,
        events.OutputFileEvent(
            zip_file,
            zip_file,
            ".zip",
            "A zip file of GIS information on gridded products",
            events.TypeOfFileLIME.GRIDDED_DATA_FOLDER,
        ),
    )
    # Create a new ZIP file
    with zipfile.ZipFile(zip_file, "w") as zf:
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
    READ_ME_Name = (
        GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
        + "/"
        + GLHE.CLAY.globals.config["LAKE_NAME"]
        + "_README"
    )
    doc = snakemd.new_doc()
    doc.add_heading(GLHE.CLAY.globals.config["LAKE_NAME"] + " README Information!")
    doc.add_paragraph(
        "The following data products have been used and have the following important information:"
    )

    list_of_data_products_readme = []
    for key in read_me_information["Data_Product"]:
        list_of_data_products_readme.append(
            key + ": " + read_me_information["Data_Product"][key]
        )
    doc.add_unordered_list(list_of_data_products_readme)
    doc.add_paragraph(
        "There are severeal files in this repo, here is the list of files:"
    )
    Items = []
    for key in read_me_information["Output_File"]:
        Items.append(
            snakemd.Inline(
                "**" + key + "**: " + read_me_information["Output_File"][key]
            )
        )
    doc.add_ordered_list(Items)
    doc.dump(READ_ME_Name)
    logger.info("Wrote README file to: " + READ_ME_Name)
    pubsub.EventBus.Publish(
        pubsub.EventBus,
        events.OutputFileEvent(
            READ_ME_Name,
            READ_ME_Name + ".md",
            ".md",
            "README FIle",
            events.TypeOfFileLIME.READ_ME,
        ),
    )
    return


def write_lake_polygon(polygon) -> None:
    """Write the lake outline to Lake_Polygon.shp (EPSG:4326) for LIME's globe."""
    import geopandas as gpd

    path = (
        GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
        + "/Lake_Polygon.shp"
    )
    gpd.GeoDataFrame(
        {"LAKE_NAME": [GLHE.CLAY.globals.config["LAKE_NAME"]]},
        geometry=[polygon],
        crs="EPSG:4326",
    ).to_file(path)
    logger.info("Wrote lake polygon to: " + path)


def write_caveats(polygon, dataset: pd.DataFrame) -> None:
    """
    Write the known data caveats for this lake to ``{lake}_caveats.json``.

    The caveats come from the queryearth rules in ``GLHE/CLAY/caveat_rules/``,
    matched on the lake polygon, the data period and the products in
    ``dataset`` (see ``GLHE/CLAY/caveats.py``). Never fails the run.
    """
    import json

    from GLHE.CLAY import caveats

    try:
        found = caveats.caveats_for_dataset(polygon, dataset)
    except Exception:
        logger.exception("Could not work out the data caveats")
        return
    filename = GLHE.CLAY.globals.config["LAKE_NAME"] + "_caveats.json"
    out_path = (
        GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/" + filename
    )
    with open(out_path, "w") as f:
        json.dump(found, f, indent=2, default=str)
    logger.info("Wrote %d data caveats to %s", len(found), out_path)
    pubsub.EventBus.Publish(
        pubsub.EventBus,
        events.OutputFileEvent(
            filename,
            out_path,
            ".json",
            "Known data caveats for this lake (queryearth rules matched on place, period and products)",
            events.TypeOfFileLIME.CAVEATS_DATA,
        ),
    )


def write_and_output_LIME_CONFIG(config_information: dict, hylak_id: int) -> None:
    """
    Create a CONFIG file that points to all exported data in LIME-readable format
    """
    CONFIG_Name = (
        GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/config.json"
    )
    optional_keys = {
        "README": events.TypeOfFileLIME.READ_ME,
        "DATA_PRODUCTS_CONFIG": events.TypeOfFileLIME.DATA_PRODUCTS_CONFIG,
        "OTHER": events.TypeOfFileLIME.OTHER,
        "SERIES_DATA": events.TypeOfFileLIME.SERIES_DATA,
        "GRIDDED_DATA_FOLDER": events.TypeOfFileLIME.GRIDDED_DATA_FOLDER,
        "NWM_LAKE_POINT_SHAPEFILENAME": events.TypeOfFileLIME.NWM_LAKE_POINT_SHAPEFILENAME,
        "WATER_BALANCE_DATA": events.TypeOfFileLIME.WATER_BALANCE_DATA,
        "TREND_STATS_DATA": events.TypeOfFileLIME.TREND_STATS_DATA,
        "HYPSOMETRY_DATA": events.TypeOfFileLIME.HYPSOMETRY_DATA,
        "CAVEATS_DATA": events.TypeOfFileLIME.CAVEATS_DATA,
    }
    ribbit = {
        "CLAY_OUTPUT_FOLDER_LOCATION": GLHE.CLAY.globals.config["DIRECTORIES"][
            "OUTPUT_DIRECTORY"
        ],
        "LAKE_NAME": GLHE.CLAY.globals.config["LAKE_NAME"],
        "HYLAK_ID": int(hylak_id),
    }
    for json_key, event_key in optional_keys.items():
        if event_key in config_information:
            ribbit[json_key] = config_information[event_key]
        else:
            logger.info("No PubSub event for %s", json_key)

    with open(CONFIG_Name, "w") as fp:
        json.dump(ribbit, fp)
    logger.info("Wrote CONFIG file to: " + CONFIG_Name)
    return


def compute_trend_statistics(dataset: pd.DataFrame) -> None:
    """Mann-Kendall trend test and Sen's slope for every data column.

    Writes {lake}_trend_stats.csv to the output directory.  Requires scipy.
    Columns with fewer than 10 valid observations are skipped.
    """
    try:
        from scipy import stats
    except ImportError:
        logger.warning("scipy not installed — skipping trend statistics")
        return

    rows = []
    for col in dataset.columns:
        series = dataset[col].dropna()
        if len(series) < 10:
            continue
        x = np.arange(len(series))
        tau, p_value = stats.kendalltau(x, series.values)
        slope, intercept, _, _ = stats.theilslopes(series.values, x)
        rows.append(
            {
                "variable": col,
                "n_months": len(series),
                "mann_kendall_tau": round(float(tau), 4),
                "p_value": round(float(p_value), 4),
                "significant_at_0.05": bool(p_value < 0.05),
                "trend_direction": "increasing" if tau > 0 else "decreasing",
                "sen_slope_per_month": round(float(slope), 6),
                "sen_slope_per_year": round(float(slope) * 12, 4),
            }
        )

    if not rows:
        logger.info("No columns with sufficient data for trend analysis")
        return

    trend_df = pd.DataFrame(rows)
    out_path = (
        GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
        + "/"
        + GLHE.CLAY.globals.config["LAKE_NAME"]
        + "_trend_stats.csv"
    )
    trend_df.to_csv(out_path, index=False)
    logger.info("Trend statistics written to %s", out_path)
    pubsub.EventBus.Publish(
        pubsub.EventBus,
        events.OutputFileEvent(
            GLHE.CLAY.globals.config["LAKE_NAME"] + "_trend_stats.csv",
            out_path,
            ".csv",
            "Mann-Kendall trend test and Sen's slope for each data variable",
            events.TypeOfFileLIME.TREND_STATS_DATA,
        ),
    )


def compute_water_balance(dataset: pd.DataFrame) -> None:
    """Compute water balance components from available data and write a summary CSV.

    Computes:
      - P_mean_mm_month    — ensemble mean of all precipitation products
      - E_mean_mm_month    — ensemble mean of all evaporation products
      - P_minus_E_mm_month — net atmospheric water input to the lake surface
      - Inflow_mean_m3_month — ensemble mean of river inflow (where available)
      - Water_Level_mean_m  — ensemble mean of water surface elevation (where available)

    P and E are in mm/month (depth over lake surface).  Inflow is in m³/month.
    Converting between depth and volume requires lake area, which is not yet
    available in this pipeline — the two quantities are reported separately.
    """
    p_cols = [c for c in dataset.columns if c.startswith("p.")]
    e_cols = [c for c in dataset.columns if c.startswith("e.")]
    i_cols = [c for c in dataset.columns if c.startswith("i.")]
    l_cols = [c for c in dataset.columns if c.startswith("l.")]

    if not p_cols and not e_cols:
        logger.info("No P or E data available — skipping water balance output")
        return

    wb = pd.DataFrame(index=dataset.index)
    if p_cols:
        wb["P_mean_mm_month"] = dataset[p_cols].mean(axis=1)
    if e_cols:
        wb["E_mean_mm_month"] = dataset[e_cols].mean(axis=1)
    if p_cols and e_cols:
        wb["P_minus_E_mm_month"] = wb["P_mean_mm_month"] - wb["E_mean_mm_month"]
    if i_cols:
        wb["Inflow_mean_m3_month"] = dataset[i_cols].mean(axis=1)
    if l_cols:
        wb["Water_Level_mean_m"] = dataset[l_cols].mean(axis=1)

    out_path = (
        GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
        + "/"
        + GLHE.CLAY.globals.config["LAKE_NAME"]
        + "_water_balance.csv"
    )
    wb.to_csv(out_path)
    logger.info("Water balance components written to %s", out_path)
    pubsub.EventBus.Publish(
        pubsub.EventBus,
        events.OutputFileEvent(
            GLHE.CLAY.globals.config["LAKE_NAME"] + "_water_balance.csv",
            out_path,
            ".csv",
            "Water balance components: P, E, P−E (mm/month), inflow (m³/month), water level (m)",
            events.TypeOfFileLIME.WATER_BALANCE_DATA,
        ),
    )


if __name__ == "__main__":
    print("This is the all_data_actions file")

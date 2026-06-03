"""
LIME — Lake Interactive Map Explorer

Entry point: LIME_driver(output_dir)

Reads config.json written by CLAY and launches a Panel dashboard with four tabs:
  1. Time Series  — interactive line charts per variable type (P / E / I / O)
  2. Data Table   — paginated, sortable table of the CSV output
  3. Gridded Data — GeoTIFF point overlay on a tile map
  4. Lake Map     — NWM validation point on a tile map
"""

import json
import panel as pn
import holoviews as hv
from pathlib import Path

from GLHE.LIME.series_data_display import SeriesDataDisplay
from GLHE.LIME.gridded_data_display import GriddedDataDisplay
from GLHE.LIME.lake_map_display import LakeMapDisplay

pn.extension("tabulator")
hv.extension("bokeh")


def _load_config(output_dir: str | None) -> dict:
    """
    Load config.json from the CLAY output directory.
    If output_dir is None, falls back to the pointer stored in LIME/config/config.json.
    """
    if output_dir is None:
        pointer_path = Path(__file__).parent / "config" / "config.json"
        with open(pointer_path) as f:
            pointer = json.load(f)
        output_dir = pointer["CLAY_OUTPUT_FOLDER_LOCATION"]
    with open(Path(output_dir) / "config.json") as f:
        return json.load(f)


def build_dashboard(config: dict) -> pn.template.FastListTemplate:
    """
    Assemble the Panel dashboard from CLAY output described by config.

    Parameters
    ----------
    config : dict
        The contents of config.json written by CLAY_driver.

    Returns
    -------
    pn.template.FastListTemplate
        A serveable Panel app.
    """
    lake_name = config["LAKE_NAME"].replace("_", " ")

    # --- Time series tab ---
    series = SeriesDataDisplay(config["SERIES_DATA"])
    ts_tab = pn.Column(*series.make_plots(), sizing_mode="stretch_width")

    # --- Data table tab ---
    table_tab = series.make_table()

    # --- Gridded data tab ---
    try:
        gridded_map = GriddedDataDisplay(config["GRIDDED_DATA_FOLDER"]).make_map()
        gridded_tab = pn.pane.HoloViews(gridded_map, sizing_mode="stretch_width")
    except Exception as e:
        gridded_tab = pn.pane.Alert(f"Gridded data unavailable: {e}", alert_type="warning")

    # --- Lake map tab ---
    lake_map = LakeMapDisplay(config).make_map()
    lake_tab = pn.pane.HoloViews(lake_map, sizing_mode="stretch_width")

    return pn.template.FastListTemplate(
        title=f"GLHE — {lake_name}",
        main=[
            pn.Tabs(
                ("Time Series", ts_tab),
                ("Data Table", table_tab),
                ("Gridded Data", gridded_tab),
                ("Lake Map", lake_tab),
            )
        ],
    )


def LIME_driver(output_dir: str | None = None) -> None:
    """
    Launch the LIME dashboard.

    Parameters
    ----------
    output_dir : str, optional
        Path to the CLAY output directory for the lake of interest.
        If None, reads the path from LIME/config/config.json.

    Example
    -------
    >>> import GLHE
    >>> clay = GLHE.CLAY.CLAY_driver.CLAY_driver()
    >>> output_dir = clay.main(67)
    >>> GLHE.LIME.LIME_sample_dashboard.LIME_driver(output_dir)
    """
    config = _load_config(output_dir)
    dashboard = build_dashboard(config)
    dashboard.show()


if __name__ == "__main__":
    LIME_driver()

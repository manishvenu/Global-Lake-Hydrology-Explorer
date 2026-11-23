"""
AppState: the single source of truth for what LIME is currently showing.

Components *read* from an AppState and react to its params changing (via
``param.depends`` / ``pn.bind``); they do not talk to each other directly.
This module is pure Python: no widgets, no Panel imports.

Typical use
-----------
>>> state = AppState()
>>> state.load_output_dir("tests/LIME/fixtures/Fixture_Lake")
>>> state.lake_name, state.df.columns.tolist()
"""

from __future__ import annotations

import json
import logging
from pathlib import Path, PureWindowsPath

import pandas as pd
import param

logger = logging.getLogger(__name__)


def resolve_path(value: str | None, output_dir: Path) -> Path | None:
    """
    Find a file referenced from config.json.

    CLAY writes absolute paths, which break when an output folder is moved
    (or copied to another machine). So: use the path as written if it exists,
    otherwise look for a file with the same name inside ``output_dir``.
    NWM point shapefiles are written without an extension, so a few common
    suffixes are tried too.
    """
    if not value:
        return None
    raw = Path(value)
    # A path written on Windows (C:\Users\me\X.csv) is one long "name" to
    # macOS/Linux, so take the file name with Windows rules when needed.
    name = PureWindowsPath(value).name if "\\" in value else raw.name
    candidates = [raw, output_dir / raw, output_dir / name]
    for base in list(candidates):
        for suffix in (".shp", ".zip", ".shp.zip"):
            candidates.append(base.with_name(base.name + suffix))
    for c in candidates:
        if c.exists():
            return c
    return None


def _read_polygon_geojson(output_dir: Path) -> dict:
    """Read Lake_Polygon.shp (if present) as a GeoJSON FeatureCollection in EPSG:4326."""
    shp = output_dir / "Lake_Polygon.shp"
    if not shp.exists():
        return {}
    import geopandas as gpd  # imported lazily: only needed here

    gdf = gpd.read_file(shp)
    if gdf.crs is not None:
        gdf = gdf.to_crs(4326)
    return json.loads(gdf.to_json())


class AppState(param.Parameterized):
    """Everything the UI needs to know about the selected lake and its CLAY run."""

    hylak_id = param.Integer(default=None, allow_None=True, doc="HydroLAKES ID")
    lake_name = param.String(default="", doc="Lake name (underscores as in CLAY)")
    output_dir = param.String(default="", doc="CLAY output folder for this lake")
    config = param.Dict(default={}, doc="Contents of the lake's config.json")
    df = param.DataFrame(
        default=None,
        allow_None=True,
        doc="Monthly series; Date index; columns like 'p.ERA5-Land'",
    )
    polygon_geojson = param.Dict(
        default={}, doc="Lake polygon as a GeoJSON FeatureCollection"
    )
    job_status = param.Selector(
        default="idle", objects=["idle", "running", "done", "failed"]
    )
    job_log = param.String(default="", doc="Text log of the current CLAY job")
    caveats = param.List(default=[], doc="Caveat dicts from caveats.find_caveats")

    # -- helpers -----------------------------------------------------------

    def path_for(self, config_key: str) -> Path | None:
        """Resolved path for a file listed in config.json (None if missing)."""
        if not self.output_dir:
            return None
        return resolve_path(self.config.get(config_key), Path(self.output_dir))

    @property
    def display_name(self) -> str:
        """Lake name with spaces, for titles."""
        return self.lake_name.replace("_", " ")

    # -- loading -----------------------------------------------------------

    def load_output_dir(self, path: str | Path) -> None:
        """
        Load a CLAY output folder (the one holding config.json) into this state.

        Missing optional files (CSV, polygon) are tolerated: the matching
        param is left empty and a warning is logged. A missing config.json
        raises FileNotFoundError.
        """
        output_dir = Path(path).expanduser().resolve()
        with open(output_dir / "config.json") as f:
            config = json.load(f)

        df = None
        csv = resolve_path(config.get("SERIES_DATA"), output_dir)
        if csv is not None:
            df = pd.read_csv(csv, index_col=0, parse_dates=True)
            df.index.name = "Date"
        else:
            logger.warning("No series CSV found for %s", output_dir)

        try:
            polygon = _read_polygon_geojson(output_dir)
        except Exception:  # a broken shapefile should not stop the app
            logger.exception("Could not read Lake_Polygon.shp in %s", output_dir)
            polygon = {}

        hylak_id = config.get("HYLAK_ID")  # optional; older configs lack it
        # Update all params in one go so watchers fire once, with consistent data.
        self.param.update(
            output_dir=str(output_dir),
            config=config,
            lake_name=config.get("LAKE_NAME", output_dir.name),
            hylak_id=int(hylak_id) if hylak_id is not None else None,
            df=df,
            polygon_geojson=polygon,
            caveats=[],
        )

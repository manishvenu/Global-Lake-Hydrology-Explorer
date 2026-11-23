"""
Look up a lake in the local HydroLAKES shapefile, by ID or by a map click.

Reads only the rows it needs (an attribute ``where`` filter or a small
bounding box) with pyogrio, so it is fast even though HydroLAKES has 1.4
million polygons. No GDAL Python bindings (``osgeo``) are required.

Both functions return ``None`` when the shapefile is missing (it lives in
``GLHE/CLAY/LocalData/``, which is downloaded separately) or nothing matches.
On success they return::

    {"hylak_id": 67, "name": "Great_Salt_Lake", "area_km2": 4360.0,
     "geojson": {...FeatureCollection in EPSG:4326...}}

``name`` uses underscores for spaces, matching CLAY's folder names, and falls
back to the ID when HydroLAKES has no name.
"""

from __future__ import annotations

import json
import logging
import warnings
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

#: Default location of the HydroLAKES polygons (same file CLAY uses).
HYDROLAKES_SHP = (
    Path(__file__).resolve().parent.parent
    / "CLAY"
    / "LocalData"
    / "HydroLAKES_polys_v10_shp"
    / "HydroLAKES_polys_v10.shp"
)

#: Half-width (degrees) of the box read around a clicked point.
CLICK_RADIUS_DEG = 0.05


def _read(shp: Path, **filters):
    """Read matching rows as a GeoDataFrame, or None if data/libraries are missing."""
    if not shp.exists():
        logger.info("HydroLAKES shapefile not found at %s", shp)
        return None
    try:
        import geopandas as gpd

        gdf = gpd.read_file(shp, engine="pyogrio", **filters)
    except Exception as exc:  # missing pyogrio, corrupt file, bad filter...
        logger.warning("HydroLAKES read failed: %s", exc)
        return None
    if gdf.empty:
        return None
    if gdf.crs is not None:
        gdf = gdf.to_crs(4326)
    return gdf


def _to_result(gdf) -> dict:
    """Turn a one-row GeoDataFrame into the lookup result dict."""
    row = gdf.iloc[0]
    hylak_id = int(row["Hylak_id"])
    raw_name = row.get("Lake_name")
    name = "" if raw_name is None or pd.isna(raw_name) else str(raw_name).strip()
    area = row.get("Lake_area")
    return {
        "hylak_id": hylak_id,
        "name": (name or str(hylak_id)).replace(" ", "_"),
        "area_km2": None if area is None or pd.isna(area) else float(area),
        "geojson": json.loads(gdf.iloc[[0]].to_json()),
    }


def lookup_by_id(hylak_id: int, shp: Path | str | None = None) -> dict | None:
    """Find a lake by its HydroLAKES ID. Returns None if missing or not found."""
    try:
        hylak_id = int(hylak_id)
    except (TypeError, ValueError):
        return None
    gdf = _read(Path(shp) if shp else HYDROLAKES_SHP, where=f"Hylak_id = {hylak_id}")
    return None if gdf is None else _to_result(gdf)


def lookup_by_point(
    lon: float, lat: float, shp: Path | str | None = None
) -> dict | None:
    """
    Find the lake under (or nearest to) a clicked point.

    Reads only lakes whose bounding box overlaps a small box around the
    point. A lake containing the point wins; otherwise the closest one in
    the box is returned. None if nothing is nearby or data is missing.
    """
    from shapely.geometry import Point

    r = CLICK_RADIUS_DEG
    bbox = (lon - r, lat - r, lon + r, lat + r)
    gdf = _read(Path(shp) if shp else HYDROLAKES_SHP, bbox=bbox)
    if gdf is None:
        return None
    pt = Point(lon, lat)
    # Areas/distances in degrees are only used to *rank* nearby lakes, so the
    # geopandas "geographic CRS" warning does not apply here.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        inside = gdf[gdf.contains(pt)]
        if not inside.empty:
            # Nested matches are rare; prefer the smallest (most specific) lake.
            return _to_result(inside.loc[[inside.geometry.area.idxmin()]])
        distances = gdf.geometry.distance(pt)
    return _to_result(gdf.loc[[distances.idxmin()]])

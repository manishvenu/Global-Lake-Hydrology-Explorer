"""
Look up a lake in HydroLAKES, by ID or by a map click.

A thin layer over ``GLHE.CLAY.hydrolakes``, which reads the HydroLAKES
GeoParquet file (a local copy if present, else straight from S3, fetching only
the rows needed). Both functions return ``None`` when HydroLAKES cannot be read
or nothing matches. On success they return::

    {"hylak_id": 67, "name": "Great_Salt_Lake", "area_km2": 4360.0,
     "geojson": {...FeatureCollection in EPSG:4326...}}

``name`` uses underscores for spaces, matching CLAY's folder names, and falls
back to the ID when HydroLAKES has no name.
"""

from __future__ import annotations

import json
import logging
import warnings

import pandas as pd

from GLHE.CLAY import hydrolakes

logger = logging.getLogger(__name__)

#: Half-width (degrees) of the box searched around a clicked point.
CLICK_RADIUS_DEG = 0.05


def available(source: str | None = None) -> bool:
    """True when HydroLAKES can be read (locally or from S3)."""
    return hydrolakes.available(source)


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


def lookup_by_id(hylak_id: int, source: str | None = None) -> dict | None:
    """Find a lake by its HydroLAKES ID. None if unreadable or not found."""
    try:
        hylak_id = int(hylak_id)
    except (TypeError, ValueError):
        return None
    if not available(source):
        return None
    try:
        gdf = hydrolakes.get_lake(hylak_id, source)
    except Exception as exc:  # network error, corrupt file...
        logger.warning("HydroLAKES lookup failed: %s", exc)
        return None
    return None if gdf is None else _to_result(gdf)


def lookup_by_point(lon: float, lat: float, source: str | None = None) -> dict | None:
    """
    Find the lake under (or nearest to) a clicked point.

    Looks only at lakes whose bounding box overlaps a small box around the
    point. A lake containing the point wins; otherwise the closest one in the
    box is returned. None if nothing is nearby or HydroLAKES is unreadable.
    """
    from shapely.geometry import Point

    if not available(source):
        return None
    try:
        gdf = hydrolakes.lakes_near(lon, lat, CLICK_RADIUS_DEG, source)
    except Exception as exc:
        logger.warning("HydroLAKES lookup failed: %s", exc)
        return None
    if gdf.empty:
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

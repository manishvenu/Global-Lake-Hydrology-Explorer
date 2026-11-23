"""
Look up a lake in HydroLAKES: by ID, by a map click, or by searching names.

A thin layer over ``GLHE.CLAY.hydrolakes``, which reads the HydroLAKES
GeoParquet file (a local copy if present, else straight from S3, fetching only
the rows needed). Both functions return ``None`` when HydroLAKES cannot be read
or nothing matches. On success they return::

    {"hylak_id": 67, "name": "Great_Salt_Lake", "area_km2": 4360.0,
     "geojson": {...FeatureCollection in EPSG:4326...}}

``name`` uses underscores for spaces, matching CLAY's folder names, and falls
back to the ID when HydroLAKES has no name.

``search()`` and ``quick_info()`` use an in-memory index (no outline, no
network once loaded) and return ``{"hylak_id", "name", "country",
"area_km2"}`` with the same ``name`` convention.
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

#: Largest HYLAK_ID in HydroLAKES v1.0.
MAX_HYLAK_ID = 1427688


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


def display_info(hylak_id, name=None, country=None, area_km2=None) -> dict:
    """The lake dict LIME uses, from raw HydroLAKES values (name may be None)."""
    hylak_id = int(hylak_id)
    name = "" if name is None or pd.isna(name) else str(name).strip()
    try:
        area = None if area_km2 is None or pd.isna(area_km2) else float(area_km2)
    except (TypeError, ValueError):
        area = None
    country = None if country is None or pd.isna(country) else str(country)
    return {
        "hylak_id": hylak_id,
        "name": (name or str(hylak_id)).replace(" ", "_"),
        "country": country or None,
        "area_km2": area,
    }


def index_ready(source: str | None = None) -> bool:
    """True once the search index is in memory (see ``app.warm_hydrolakes``)."""
    return hydrolakes.index_ready(source)


def quick_info(hylak_id: int, source: str | None = None) -> dict | None:
    """Name, country and area from the index, or None if it is not loaded yet."""
    if not index_ready(source):
        return None
    hit = hydrolakes.lake_index(source).info(hylak_id)
    return display_info(**hit) if hit else None


def search(text: str, limit: int = 8, source: str | None = None) -> list[dict] | None:
    """
    Lakes matching ``text`` (a name, a country or a HYLAK_ID), best first.

    Returns None while the index is still loading (except for a plain number,
    which is offered as an ID so typing an ID always works), and [] when
    nothing matches or HydroLAKES cannot be read.
    """
    text = str(text or "").strip().lstrip("#")
    if not text:
        return []
    if not index_ready(source):
        if text.isdigit() and 1 <= int(text) <= MAX_HYLAK_ID:
            return [{"hylak_id": int(text), "name": None}]
        return None
    try:
        return [display_info(**hit) for hit in hydrolakes.search(text, limit, source)]
    except Exception as exc:
        logger.warning("HydroLAKES search failed: %s", exc)
        return []

"""
Known data caveats for a lake, found with queryearth.

A *caveat* is a documented issue with a data product that applies to some
place, some period and/or some products: for example "ERA5-Land monthly
accumulations are wrong from Sep 2022 to Feb 2024". The caveats live as
queryearth rule files (GeoJSON) in ``GLHE/LIME/rules/``; see the README there.

This module is pure Python (no Panel). The main entry point is
:func:`find_caveats`; :func:`caveats_for_state` is a convenience wrapper that
reads the lake polygon, dates and products from an ``AppState``.

Example
-------
>>> from shapely.geometry import box
>>> found = find_caveats(box(-112.6, 41.0, -112.2, 41.4),
...                      "2022-01-01", "2024-12-31", ["ERA5-Land"])
>>> [c["name"] for c in found][:1]
['ERA5LAND/ERA5LAND001']
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import queryearth as qe
from queryearth import space as sp

logger = logging.getLogger(__name__)

#: Folder holding the rule files shipped with LIME.
RULES_DIR = Path(__file__).parent / "rules"

#: Which rule files to load, and which GLHE products each one is about.
#: ``products=None`` means the file scopes its rules by the ``product``
#: dimension itself, so it is queried for every product.
#: ``vizqueryearth_src`` is the file's path on the vizqueryearth site, for
#: files vendored from there (None for our own files).
RULE_FILES: dict[str, dict[str, Any]] = {
    "era5-land.geojson": {
        "products": ["ERA5-Land"],
        "vizqueryearth_src": "sites/datasets/era5-land.geojson",
    },
    "glhe.geojson": {
        "products": None,
        "vizqueryearth_src": None,
    },
}

#: GLHE product names, as in the CSV column names (see data_products.json).
KNOWN_PRODUCTS = ["ERA5-Land", "CRUTS", "NWM"]

#: Rules that explain why a product is *missing* (e.g. "NWM covers the
#: contiguous US only"). They are checked for every known product, because
#: the lakes they are about have no columns for that product.
COVERAGE_RULES = {"GLHE/NWM002"}

#: Base URL of the vizqueryearth web viewer.
VIZQUERYEARTH_URL = "https://crocodile-cesm.github.io/vizqueryearth/"

#: Sort order for rule levels: most severe first.
LEVEL_ORDER = {"error": 0, "warning": 1, "info": 2}


def _dimensions() -> list:
    """
    The extra (non space/time) dimensions our rule files may use.

    - ``product``: GLHE product name, as in the CSV columns ("ERA5-Land", "CRUTS", "NWM").
    - ``lake_area``: lake surface area in km2.
    """
    return [qe.Labels("product"), qe.Axis("lake_area", "km2")]


@lru_cache(maxsize=None)
def load_registry(rule_file: str) -> qe.Registry:
    """Read one rule file from :data:`RULES_DIR` (cached after the first call)."""
    return qe.Registry.from_file(RULES_DIR / rule_file, dimensions=_dimensions())


def vizqueryearth_url(rule_file: str) -> str | None:
    """
    Link to view a vendored rule file on the vizqueryearth map, or None.

    >>> vizqueryearth_url("era5-land.geojson")
    'https://crocodile-cesm.github.io/vizqueryearth/?src=sites/datasets/era5-land.geojson'
    """
    src = RULE_FILES.get(Path(rule_file).name, {}).get("vizqueryearth_src")
    return f"{VIZQUERYEARTH_URL}?src={src}" if src else None


# ---------------------------------------------------------------------------
# Turning GLHE values into query values
# ---------------------------------------------------------------------------


def normalize_product(name: str) -> str:
    """GLHE product name as used in CSV columns: 'ERA5_Land' -> 'ERA5-Land'."""
    return name.strip().replace("_", "-")


def products_from_columns(columns: Iterable[str]) -> list[str]:
    """
    Product names found in CSV column names.

    >>> products_from_columns(["p.ERA5-Land", "e.CRUTS", "p.CRUTS"])
    ['ERA5-Land', 'CRUTS']
    """
    found: list[str] = []
    for col in columns:
        if "." in str(col):
            product = normalize_product(str(col).split(".", 1)[1])
            if product not in found:
                found.append(product)
    return found


def _to_shapely(polygon):
    """Accept a shapely geometry or GeoJSON (geometry, Feature or FeatureCollection)."""
    if polygon is None or polygon == {}:
        return None
    if hasattr(polygon, "geom_type"):
        return polygon
    import shapely
    from shapely.geometry import shape

    if polygon.get("type") == "FeatureCollection":
        geoms = [shape(f["geometry"]) for f in polygon.get("features", [])]
        geoms = [g for g in geoms if g is not None and not g.is_empty]
        return shapely.union_all(geoms) if geoms else None
    if polygon.get("type") == "Feature":
        return shape(polygon["geometry"]) if polygon.get("geometry") else None
    return shape(polygon)


def lake_area_km2(polygon) -> float:
    """Geodesic area of a lon/lat polygon in km2 (0 for points and lines)."""
    geom = _to_shapely(polygon)
    if geom is None or geom.geom_type not in ("Polygon", "MultiPolygon"):
        return 0.0
    from pyproj import Geod

    area_m2, _ = Geod(ellps="WGS84").geometry_area_perimeter(geom)
    return abs(area_m2) / 1e6


def _to_space(geom):
    """queryearth space value for a shapely geometry in lon/lat degrees."""
    if geom.geom_type in ("Polygon", "MultiPolygon"):
        return sp.polygon(geom)
    if geom.geom_type == "Point":
        return sp.point(geom.x, geom.y)
    return sp.box(*geom.bounds)  # lines etc.: use their bounding box


def _to_date(value) -> str | None:
    """ISO date string (YYYY-MM-DD) from a str, date or Timestamp; None stays None."""
    if value is None:
        return None
    import pandas as pd

    return pd.Timestamp(value).date().isoformat()


# ---------------------------------------------------------------------------
# Finding caveats
# ---------------------------------------------------------------------------


def _rule_to_dict(rule, rule_file: str) -> dict:
    """Flatten a queryearth Rule into the plain dict the UI uses."""
    feature = rule.to_feature()
    props = feature.get("properties", {})
    time = feature.get("time") or {}
    refs = [
        {"title": r.get("title") or r.get("url", ""), "url": r.get("url", "")}
        for r in props.get("references", []) or []
        if isinstance(r, dict)
    ]
    return {
        "name": rule.name,
        "title": rule.title,
        "message": rule.message,
        "level": rule.level,
        "instruction": props.get("instruction"),
        "references": refs,
        "source_file": rule_file,
        "vizqueryearth_url": vizqueryearth_url(rule_file),
        "time": time.get("interval"),  # [start, end] or None
        "geometry": feature.get("geometry"),  # GeoJSON or None
    }


def find_caveats(
    polygon,
    start=None,
    end=None,
    products: Iterable[str] | None = None,
    lake_area: float | None = None,
) -> list[dict]:
    """
    Caveats that apply to a lake, a period and a set of data products.

    Parameters
    ----------
    polygon : shapely geometry or GeoJSON dict, optional
        Lake outline in lon/lat (EPSG:4326). A Point also works. If None,
        rules tied to a region are not matched.
    start, end : str, date or Timestamp, optional
        The data period. If either is missing, rules tied to a period are not
        matched.
    products : list of str, optional
        GLHE product names, e.g. ``["ERA5-Land", "CRUTS"]``. ``"ERA5_Land"``
        is accepted too. None means all of :data:`KNOWN_PRODUCTS`.
    lake_area : float, optional
        Lake area in km2. Computed from ``polygon`` when not given.

    Returns
    -------
    list of dict
        One dict per matching rule with keys ``name``, ``title``,
        ``message``, ``level``, ``instruction``, ``references``
        (``[{title, url}]``), ``source_file``, ``vizqueryearth_url``,
        ``time`` and ``geometry`` (GeoJSON or None); errors first, then
        warnings, then info.
    """
    geom = _to_shapely(polygon)
    wanted = [normalize_product(p) for p in (products or KNOWN_PRODUCTS)]
    if lake_area is None and geom is not None:
        lake_area = lake_area_km2(geom)

    query: dict[str, Any] = {}
    if geom is not None and not geom.is_empty:
        query["space"] = _to_space(geom)
    if start is not None and end is not None:
        query["time"] = qe.interval(_to_date(start), _to_date(end))
    if lake_area is not None:
        query["lake_area"] = float(lake_area)

    results: dict[str, dict] = {}
    for rule_file, info in RULE_FILES.items():
        # Skip a file that is only about products this lake does not use.
        file_products = info["products"] or wanted
        products_here = [p for p in wanted if p in file_products]
        if not products_here:
            continue
        file_query = {**query, "product": products_here}

        try:
            matched = load_registry(rule_file).query(**file_query)
        except Exception:  # a broken rule file must not break the app
            logger.exception("Could not query caveat rules in %s", rule_file)
            continue
        for rule in matched:
            results.setdefault(rule.name, _rule_to_dict(rule, rule_file))

    # Coverage rules: ask again with every known product, keep only those.
    if any(p not in wanted for p in KNOWN_PRODUCTS):
        for rule_file in RULE_FILES:
            try:
                matched = load_registry(rule_file).query(
                    **query, product=list(KNOWN_PRODUCTS)
                )
            except Exception:  # a broken rule file must not break the app
                logger.exception("Could not query caveat rules in %s", rule_file)
                continue
            for rule in matched:
                if rule.name in COVERAGE_RULES:
                    results.setdefault(rule.name, _rule_to_dict(rule, rule_file))

    return sorted(
        results.values(),
        key=lambda c: (LEVEL_ORDER.get(c["level"], 9), c["name"]),
    )


def caveats_for_state(state) -> list[dict]:
    """
    :func:`find_caveats` for the lake currently loaded in an ``AppState``.

    Uses ``state.polygon_geojson``, the first/last date in ``state.df`` and
    the products in its column names. Does not modify ``state``; the caller
    sets ``state.caveats`` with the result.
    """
    df = state.df
    start = end = None
    products = None
    if df is not None:
        # Products come from the column names, even if there are no rows yet.
        products = products_from_columns(df.columns) or None
        if len(df.index):
            start, end = df.index.min(), df.index.max()
    return find_caveats(state.polygon_geojson or None, start, end, products)


def caveats_to_geojson(caveats: list[dict]) -> dict:
    """
    FeatureCollection of the caveats that have a region, for the globe.

    Each feature's properties hold ``name``, ``title`` and ``level``.
    """
    features = [
        {
            "type": "Feature",
            "geometry": c["geometry"],
            "properties": {k: c[k] for k in ("name", "title", "level")},
        }
        for c in caveats
        if c.get("geometry")
    ]
    return {"type": "FeatureCollection", "features": features} if features else {}


if __name__ == "__main__":  # quick manual check: python -m GLHE.LIME.caveats
    from shapely.geometry import box

    for c in find_caveats(box(-112.6, 41.0, -112.2, 41.4), "2022-01", "2024-12"):
        print(json.dumps({k: c[k] for k in ("name", "level", "title")}))

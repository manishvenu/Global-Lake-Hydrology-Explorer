"""
The globe: a MapLibre map wrapped as a Panel component (JavaScript in globe.js).

Python sets these params and the map redraws:
    lake_geojson    the selected lake's outline (the map flies to it)
    points_geojson  points, e.g. the NWM lake point
    cells_geojson   gridded cells; each feature has properties["color"]
The browser sets this param when the user clicks a lake on the tiles:
    picked          {hylak_id, name, country, area_km2}

All GeoJSON is a plain dict FeatureCollection in EPSG:4326; {} means nothing.
"""

import os
import pathlib

import panel as pn
import param

#: Every HydroLAKES lake as vector tiles (one PMTiles file, built by
#: CLAY/data_access/aws_zarr_templates/hydrolakes_to_pmtiles.py).
DEFAULT_TILES_URL = (
    "https://glhe.s3.us-east-1.amazonaws.com/HydroLAKES/HydroLAKES_polys_v10.pmtiles"
)


def lake_tiles_url() -> str:
    """$GLHE_LAKE_TILES_URL if set ("" turns the lake tiles off), else S3."""
    return os.environ.get("GLHE_LAKE_TILES_URL", DEFAULT_TILES_URL).strip()


class LakeGlobe(pn.custom.JSComponent):
    lake_geojson = param.Dict(default={})
    points_geojson = param.Dict(default={})
    cells_geojson = param.Dict(default={})
    picked = param.Dict(default={})
    lake_tiles_url = param.String(default="")

    _esm = pathlib.Path(__file__).with_name("globe.js")
    _stylesheets = [
        "https://cdn.jsdelivr.net/npm/maplibre-gl@5.6.0/dist/maplibre-gl.css",
        ":host { display: block; } .map { width: 100%; height: 100%; }",
    ]

"""
Gridded data on the globe.

CLAY saves one GeoTIFF per variable and product in a zip, named like
``p_ERA5-Land_Great_Salt_Lake_20000101.tif``. This turns one of them into
coloured cell rectangles for ``LakeGlobe.cells_geojson``.
"""

import zipfile
from pathlib import Path

import numpy as np
from bokeh.palettes import Viridis256


def list_layers(zip_path) -> list[str]:
    """The tif files inside CLAY's gridded zip."""
    with zipfile.ZipFile(zip_path) as zf:
        return sorted(n for n in zf.namelist() if n.lower().endswith(".tif"))


def cells_geojson(zip_path, member: str) -> dict:
    """One coloured rectangle per grid cell that has a value."""
    import rasterio

    with rasterio.open(f"zip://{Path(zip_path).resolve()}!/{member}") as src:
        data = src.read(1).astype(float)
        if src.nodata is not None:
            data[data == src.nodata] = np.nan
        t = src.transform

    rows, cols = np.nonzero(np.isfinite(data))
    values = data[rows, cols]
    if values.size == 0:
        return {}
    lo, hi = values.min(), values.max()
    scaled = (values - lo) / ((hi - lo) or 1)  # 0..1
    features = []
    for r, c, v, s in zip(rows, cols, values, scaled):
        # t.c, t.f: corner of the raster; t.a, t.e: cell width and height.
        west, north = t.c + c * t.a, t.f + r * t.e
        east, south = west + t.a, north + t.e
        ring = [
            [west, south],
            [east, south],
            [east, north],
            [west, north],
            [west, south],
        ]
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [ring]},
                "properties": {"value": float(v), "color": Viridis256[int(s * 255)]},
            }
        )
    return {"type": "FeatureCollection", "features": features}

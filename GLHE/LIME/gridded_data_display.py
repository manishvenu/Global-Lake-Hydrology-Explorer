import os
import zipfile
from pathlib import Path

import numpy as np
import rasterio
import holoviews as hv


class GriddedDataDisplay:
    """Reads GeoTIFFs from CLAY's output zip and renders them as point overlays on a tile map."""

    def __init__(self, zip_path: str):
        self._tifs: dict[str, str] = {}
        os.makedirs(".temp/tifs", exist_ok=True)
        with zipfile.ZipFile(zip_path) as zf:
            for name in zf.namelist():
                # Filename format: {slc}_{product}_{lake}_{date}.tif
                parts = name.split("_")
                key = parts[0] + "." + parts[1]  # e.g. "p.CRUTS"
                dest = Path(".temp/tifs") / name
                if not dest.exists():
                    zf.extract(name, ".temp/tifs")
                self._tifs[key] = str(dest)

    def _tif_to_points(self, path: str) -> hv.Points:
        """Convert a GeoTIFF to HoloViews Points in Web Mercator projection."""
        with rasterio.open(path) as src:
            data = src.read(1).astype(float)
            nodata = src.nodata
            transform = src.transform
        if nodata is not None:
            data[data == nodata] = np.nan
        rows, col_idx = np.where(~np.isnan(data))
        lons, lats = rasterio.transform.xy(transform, rows, col_idx)
        z = data[rows, col_idx]
        eastings, northings = hv.Tiles.lon_lat_to_easting_northing(
            np.array(lons), np.array(lats)
        )
        return hv.Points(
            {"easting": eastings, "northing": northings, "z": z},
            kdims=["easting", "northing"],
            vdims=["z"],
        )

    def make_map(self) -> hv.Overlay:
        """Tile base layer with one coloured point cloud per data product."""
        tiles = hv.element.tiles.CartoLight()
        overlay = tiles
        for key, path in self._tifs.items():
            try:
                points = self._tif_to_points(path).opts(
                    color="z",
                    colorbar=True,
                    size=8,
                    tools=["hover"],
                    width=700,
                    height=450,
                    title=f"Gridded Data — {key}",
                )
                overlay = overlay * points
            except Exception as e:
                print(f"Could not render {key}: {e}")
        return overlay

"""
Generate a small synthetic CLAY output folder for LIME development and tests.

It mimics what ``CLAY_driver().main()`` writes for a lake near Great Salt Lake,
so LIME can be developed without the (large) real input data.

Run from anywhere::

    python tests/LIME/fixtures/make_fixture.py

Output: ``tests/LIME/fixtures/Fixture_Lake/``. Re-running overwrites it; the
numbers are seeded, so the files are reproducible.

Notes on the format (kept identical to CLAY where possible):

* ``config.json`` keys come from ``write_and_output_LIME_CONFIG``. CLAY writes
  absolute paths; here they are bare file names, and
  ``GLHE.LIME.app.find_file`` resolves them relative to the folder.
  ``HYLAK_ID`` is an extra, optional key that real CLAY configs do not have yet.
* The CSV is named ``{LAKE}_Data.csv``, Date index, columns ``{slc}.{product}``.
* The gridded zip is ``{LAKE}_gridded_data_layers_on_{YYYYMMDD}.zip`` holding
  ``{slc}_{product}_{LAKE}_{YYYYMMDD}.tif`` GeoTIFFs (EPSG:4326).
* The NWM point is ``{LAKE}_NWM_lake_point.shp``.
"""

from __future__ import annotations

import json
import shutil
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_bounds
from shapely.geometry import Point, Polygon

LAKE = "Fixture_Lake"
HYLAK_ID = 999999
OUT = Path(__file__).parent / LAKE
BOUNDS = (-112.9, 40.7, -111.9, 41.7)  # lon_min, lat_min, lon_max, lat_max
GRID_DATE = pd.Timestamp("2000-01-01")
rng = np.random.default_rng(42)


def lake_polygon() -> Polygon:
    """An irregular, roughly lake-shaped polygon inside BOUNDS."""
    cx, cy = -112.4, 41.2
    angles = np.linspace(0, 2 * np.pi, 40, endpoint=False)
    radius = 0.38 + 0.07 * np.sin(3 * angles) + 0.03 * np.cos(5 * angles)
    xs = cx + radius * np.cos(angles) * 0.9
    ys = cy + radius * np.sin(angles) * 1.15
    return Polygon(zip(xs, ys))


def monthly_series() -> pd.DataFrame:
    """Plausible monthly P/E (mm/month) and NWM inflow (m3/month), 2000-2024."""
    dates = pd.date_range("2000-01-01", "2024-12-01", freq="MS")
    month = dates.month.to_numpy()
    # Semi-arid: wetter winter/spring, strong summer evaporation.
    p_clim = 35 + 18 * np.cos(2 * np.pi * (month - 3) / 12)
    e_clim = 60 + 55 * np.cos(2 * np.pi * (month - 7) / 12)
    n = len(dates)

    p_era = np.clip(p_clim * rng.gamma(4, 0.25, n), 0, None)
    p_cru = np.clip(p_era * rng.normal(0.92, 0.08, n), 0, None)
    e_era = np.clip(e_clim + rng.normal(0, 6, n), 0, None)
    e_cru = np.clip(e_era * rng.normal(1.08, 0.05, n), 0, None)
    i_nwm = np.clip(
        (1.2e8 + 1.4e8 * np.cos(2 * np.pi * (month - 5) / 12)) * rng.gamma(5, 0.2, n),
        0,
        None,
    )

    df = pd.DataFrame(
        {
            "p.ERA5-Land": p_era,
            "p.CRUTS": p_cru,
            "e.ERA5-Land": e_era,
            "e.CRUTS": e_cru,
            "i.NWM": i_nwm,
        },
        index=pd.DatetimeIndex(dates, name="Date"),
    ).round(3)
    # CRUTS ends earlier than ERA5-Land, like the real product; NWM starts later.
    df.loc["2023-01-01":, ["p.CRUTS", "e.CRUTS"]] = np.nan
    df.loc[:"2001-12-01", "i.NWM"] = np.nan
    return df


def write_geotiffs(folder: Path) -> list[Path]:
    """Small coarse grids per product, covering BOUNDS."""
    stamp = GRID_DATE.strftime("%Y%m%d")
    specs = [  # (slc, product, n cells per side, base value)
        ("p", "ERA5-Land", 10, 30.0),
        ("e", "ERA5-Land", 10, 25.0),
        ("p", "CRUTS", 2, 28.0),
        ("e", "CRUTS", 2, 22.0),
    ]
    paths = []
    for slc, product, n, base in specs:
        yy, xx = np.mgrid[0:n, 0:n]
        data = base + 4 * np.sin(xx / max(n - 1, 1) * 3) + 3 * yy / max(n - 1, 1)
        data = (data + rng.normal(0, 0.5, data.shape)).astype("float32")
        path = folder / f"{slc}_{product}_{LAKE}_{stamp}.tif"
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            height=n,
            width=n,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_bounds(*BOUNDS, n, n),
        ) as dst:
            dst.write(data, 1)
        paths.append(path)
    return paths


def main() -> None:
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)

    csv_name = f"{LAKE}_Data.csv"
    monthly_series().to_csv(OUT / csv_name)

    poly = lake_polygon()
    gpd.GeoDataFrame({"Lake_name": [LAKE]}, geometry=[poly], crs=4326).to_file(
        OUT / "Lake_Polygon.shp"
    )
    nwm_name = f"{LAKE}_NWM_lake_point.shp"
    gpd.GeoDataFrame(
        geometry=[Point(poly.centroid.x, poly.centroid.y)], crs=4326
    ).to_file(OUT / nwm_name)

    zip_name = f"{LAKE}_gridded_data_layers_on_{GRID_DATE.strftime('%Y%m%d')}.zip"
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(OUT / zip_name, "w") as zf:
            for tif in write_geotiffs(Path(tmp)):
                zf.write(tif, tif.name)

    config = {
        # CLAY writes an absolute path here; "." keeps the fixture portable.
        "CLAY_OUTPUT_FOLDER_LOCATION": ".",
        "LAKE_NAME": LAKE,
        "HYLAK_ID": HYLAK_ID,
        "SERIES_DATA": csv_name,
        "GRIDDED_DATA_FOLDER": zip_name,
        "NWM_LAKE_POINT_SHAPEFILENAME": nwm_name,
    }
    with open(OUT / "config.json", "w") as f:
        json.dump(config, f, indent=2)
    print(f"Wrote fixture to {OUT}")


if __name__ == "__main__":
    main()

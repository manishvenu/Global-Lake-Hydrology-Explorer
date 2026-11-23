"""
Build the HydroLAKES vector tiles (PMTiles) that LIME's globe draws.

Reads the HydroLAKES GeoParquet file one row group at a time, writes a
FlatGeobuf holding only what the globe needs (Hylak_id, Lake_name, Country,
Lake_area), then runs tippecanoe. Takes about 30 minutes and ~3 GB of disk.

Needs tippecanoe (conda-forge), geopandas, pyogrio and pyarrow, e.g.::

    conda create -n glhe-tiles -c conda-forge tippecanoe geopandas pyogrio pyarrow
    conda run -n glhe-tiles python hydrolakes_to_pmtiles.py ~/glhe-tiles

Output: <folder>/HydroLAKES_polys_v10.pmtiles (layer "lakes", feature id =
Hylak_id, zooms 0-12). Upload it next to the parquet file on S3 (see
deploy/README.md, "Lake tiles").
"""

import os
import shutil
import subprocess
import sys
import time

import geopandas as gpd
import pyarrow.parquet as pq
import pyogrio
import shapely

from GLHE.CLAY import hydrolakes

COLUMNS = ["Hylak_id", "Lake_name", "Country", "Lake_area", "geometry"]


def main(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    fgb = os.path.join(out_dir, "hydrolakes_lite.fgb")
    pmtiles = os.path.join(out_dir, "HydroLAKES_polys_v10.pmtiles")
    fs, path = hydrolakes._filesystem(hydrolakes.source())
    f = pq.ParquetFile(path, filesystem=fs)
    t0 = time.time()
    if os.path.exists(fgb):
        os.remove(fgb)
    n = f.metadata.num_row_groups
    for i in range(n):
        t = f.read_row_group(i, columns=COLUMNS)
        geometry = shapely.from_wkb(t.column("geometry").to_numpy(zero_copy_only=False))
        gdf = gpd.GeoDataFrame(
            t.drop(["geometry"]).to_pandas(), geometry=geometry, crs=4326
        )
        pyogrio.write_dataframe(
            gdf,
            fgb,
            driver="FlatGeobuf",
            append=os.path.exists(fgb),
            SPATIAL_INDEX="NO",
        )
        if i % 20 == 0:
            print(f"fgb {i + 1}/{n} {time.time() - t0:.0f}s", flush=True)

    tippecanoe = shutil.which("tippecanoe") or os.path.join(
        os.path.dirname(sys.executable), "tippecanoe"
    )
    cmd = [
        tippecanoe, "-o", pmtiles, "--force", "-l", "lakes", "-Z0", "-z12",
        "--use-attribute-for-id=Hylak_id",
        "-y", "Hylak_id", "-y", "Lake_name", "-y", "Country", "-y", "Lake_area",
        "--drop-smallest-as-needed", "--extend-zooms-if-still-dropping",
        "--simplification=4", "--no-tile-stats", fgb,
    ]  # fmt: skip
    subprocess.run(cmd, check=True)
    os.remove(fgb)
    print(f"DONE {os.path.getsize(pmtiles) / 1e9:.2f} GB {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main(os.path.expanduser(sys.argv[1] if len(sys.argv) > 1 else "~/glhe-tiles"))

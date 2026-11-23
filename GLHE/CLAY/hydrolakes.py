"""
HydroLAKES lake polygons, read from a cloud-friendly GeoParquet file.

The file (``HydroLAKES_polys_v10.parquet``) holds all 1.4 million HydroLAKES
polygons, sorted by ``Hylak_id`` in small row groups, with a ``bbox`` column
per lake. That layout lets us read only what we need, straight from S3:

* by ID: Parquet statistics point to the one row group holding that ID
  (about 1 MB is read);
* by map click: the small ``Hylak_id`` + ``bbox`` columns are read once
  (and cached), then only the few lakes whose box contains the point.

Where the file is read from (first that applies):

1. the ``GLHE_HYDROLAKES`` environment variable (a local path or ``s3://`` URL);
2. ``GLHE/CLAY/LocalData/HydroLAKES_polys_v10.parquet`` if it exists;
3. the public bucket, ``s3://glhe/HydroLAKES/HydroLAKES_polys_v10.parquet``.

Used by CLAY (``lake_extraction``) and the LIME web app (``lake_lookup``).
Needs pyarrow, shapely and geopandas, plus s3fs for S3; no GDAL bindings.
"""

from __future__ import annotations

import functools
import json
import os
from pathlib import Path

import numpy as np

FILENAME = "HydroLAKES_polys_v10.parquet"
S3_URL = "s3://glhe/HydroLAKES/" + FILENAME
LOCAL_PATH = Path(__file__).resolve().parent / "LocalData" / FILENAME
ENV_VAR = "GLHE_HYDROLAKES"

#: Rows per Parquet row group (smaller = less read per lookup, bigger file).
ROW_GROUP_SIZE = 5_000
#: At most this many candidate lakes are fetched for one map click.
MAX_CANDIDATES = 200


def source() -> str:
    """Where HydroLAKES is read from (see the module docstring)."""
    if os.environ.get(ENV_VAR):
        return os.environ[ENV_VAR]
    if LOCAL_PATH.exists():
        return str(LOCAL_PATH)
    return S3_URL


def _filesystem(src: str):
    """(filesystem, path) for pyarrow: anonymous s3fs for s3:// URLs, else local."""
    if src.startswith("s3://"):
        import s3fs

        return s3fs.S3FileSystem(anon=True), src[len("s3://") :]
    return None, src


@functools.lru_cache(maxsize=8)
def available(src: str | None = None) -> bool:
    """True if the HydroLAKES file can be opened (checked once per source)."""
    src = src or source()
    try:
        fs, path = _filesystem(src)
        return fs.exists(path) if fs else Path(path).exists()
    except Exception:
        return False


def read_lakes(hylak_ids, src: str | None = None):
    """
    Read the given lakes as a GeoDataFrame (EPSG:4326), in ``Hylak_id`` order.

    Missing IDs are simply absent from the result.
    """
    import geopandas as gpd
    import pyarrow.parquet as pq
    import shapely

    ids = sorted({int(i) for i in hylak_ids})
    fs, path = _filesystem(src or source())
    columns = [n for n in pq.read_schema(path, filesystem=fs).names if n != "bbox"]
    table = pq.read_table(
        path, filesystem=fs, columns=columns, filters=[("Hylak_id", "in", ids)]
    )
    df = table.drop(["geometry"]).to_pandas()
    geometry = shapely.from_wkb(table.column("geometry").to_numpy(zero_copy_only=False))
    return gpd.GeoDataFrame(df, geometry=geometry, crs=4326)


def get_lake(hylak_id: int, src: str | None = None):
    """One lake as a one-row GeoDataFrame, or None if the ID does not exist."""
    gdf = read_lakes([hylak_id], src)
    return None if gdf.empty else gdf


@functools.lru_cache(maxsize=4)
def _bbox_index(src: str):
    """All lakes' IDs and bounding boxes as numpy arrays (~50 MB, read once)."""
    import pyarrow.parquet as pq

    fs, path = _filesystem(src)
    t = pq.read_table(path, filesystem=fs, columns=["Hylak_id", "bbox"])
    box = t.column("bbox").combine_chunks()
    return (
        t.column("Hylak_id").to_numpy(),
        *(box.field(k).to_numpy() for k in ("xmin", "ymin", "xmax", "ymax")),
    )


def lakes_near(lon: float, lat: float, radius: float, src: str | None = None):
    """
    Lakes whose bounding box overlaps a square of half-width ``radius`` (degrees)
    around (lon, lat), as a GeoDataFrame (possibly empty). Smallest boxes first,
    capped at ``MAX_CANDIDATES``.
    """
    ids, xmin, ymin, xmax, ymax = _bbox_index(src or source())
    hit = (xmin <= lon + radius) & (xmax >= lon - radius)
    hit &= (ymin <= lat + radius) & (ymax >= lat - radius)
    idx = np.flatnonzero(hit)
    if idx.size > MAX_CANDIDATES:
        area = (xmax[idx] - xmin[idx]) * (ymax[idx] - ymin[idx])
        idx = idx[np.argsort(area)[:MAX_CANDIDATES]]
    return read_lakes(ids[idx].tolist(), src)


# --------------------------------------------------------------------- writing
def to_arrow(gdf):
    """A GeoDataFrame as an Arrow table in this module's layout (bbox + WKB geometry)."""
    import pyarrow as pa
    import shapely

    geom = np.asarray(gdf.geometry.values)
    b = shapely.bounds(geom)
    cols = {c: pa.Array.from_pandas(gdf[c]) for c in gdf.columns if c != "geometry"}
    cols["bbox"] = pa.StructArray.from_arrays(
        [pa.array(b[:, i]) for i in range(4)], names=["xmin", "ymin", "xmax", "ymax"]
    )
    cols["geometry"] = pa.array(shapely.to_wkb(geom), type=pa.binary())
    return pa.table(cols)


def geo_metadata(bounds, geometry_types) -> bytes:
    """GeoParquet 1.1 ``geo`` metadata, declaring the ``bbox`` covering column."""
    return json.dumps(
        {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": sorted(geometry_types),
                    "bbox": [float(v) for v in bounds],
                    "covering": {
                        "bbox": {
                            k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")
                        }
                    },
                }
            },
        }
    ).encode()


def write_parquet(gdf, path) -> None:
    """Write a (small) GeoDataFrame in this module's layout. Used by tests."""
    import pyarrow.parquet as pq

    gdf = gdf.to_crs(4326).sort_values("Hylak_id")
    table = to_arrow(gdf)
    meta = geo_metadata(gdf.total_bounds, set(gdf.geom_type))
    table = table.replace_schema_metadata({b"geo": meta})
    pq.write_table(table, path, compression="zstd", row_group_size=ROW_GROUP_SIZE)

"""
HydroLAKES lake polygons, read from a cloud-friendly GeoParquet file.

The file (``HydroLAKES_polys_v10.parquet``) holds all 1.4 million HydroLAKES
polygons, sorted by ``Hylak_id`` in small row groups, with a ``bbox`` column
per lake. That layout lets us read only what we need, straight from S3:

* by ID: the file is sorted, so ID ``n`` is in row group ``(n-1) // 5000``
  (about 1 MB is read; the open file and recent lakes are cached);
* by map click: the small ``Hylak_id`` + ``bbox`` columns are read once
  (and cached), then only the few lakes whose box contains the point;
* by name: ``search()`` uses an in-memory index of every lake's ID, name,
  country and area (read once, a few seconds from S3).

Where the file is read from (first that applies):

1. the ``GLHE_HYDROLAKES`` environment variable (a local path or ``s3://`` URL);
2. ``GLHE/CLAY/LocalData/HydroLAKES_polys_v10.parquet`` if it exists;
3. the public bucket, ``s3://glhe/HydroLAKES/HydroLAKES_polys_v10.parquet``.

Used by CLAY (``lake_extraction``) and the LIME web app (``lake_lookup``).
Needs pyarrow, shapely and geopandas; no GDAL bindings.
"""

from __future__ import annotations

import functools
import json
import os
import threading
import unicodedata
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


@functools.lru_cache(maxsize=8)
def _s3(bucket: str):
    """Anonymous pyarrow S3 client for a bucket (its region looked up once).

    pyarrow's own S3 client fetches the many small Parquet pieces in parallel,
    roughly 10x faster here than going through s3fs.
    """
    import pyarrow.fs as pafs

    return pafs.S3FileSystem(anonymous=True, region=pafs.resolve_s3_region(bucket))


def _filesystem(src: str):
    """(filesystem, path) for pyarrow: anonymous S3 for s3:// URLs, else local."""
    if src.startswith("s3://"):
        path = src[len("s3://") :]
        return _s3(path.split("/", 1)[0]), path
    return None, src


_AVAILABLE: set[str] = set()


def available(src: str | None = None) -> bool:
    """True if the HydroLAKES file can be opened.

    Success is remembered; failures are not, so a network blip at start-up
    does not disable lookups for the life of the server.
    """
    import pyarrow.fs as pafs

    src = src or source()
    if src in _AVAILABLE:
        return True
    try:
        fs, path = _filesystem(src)
        ok = (
            fs.get_file_info(path).type == pafs.FileType.File
            if fs
            else Path(path).exists()
        )
    except Exception:
        ok = False
    if ok:
        _AVAILABLE.add(src)
    return ok


_READ_LOCK = threading.Lock()


@functools.lru_cache(maxsize=4)
def _parquet_file(src: str):
    """The HydroLAKES file opened once per source (footer and schema cached)."""
    import pyarrow.parquet as pq

    fs, path = _filesystem(src)
    return pq.ParquetFile(path, filesystem=fs) if fs else pq.ParquetFile(path)


def _to_gdf(table):
    """Arrow table (with WKB ``geometry``, maybe ``bbox``) -> GeoDataFrame."""
    import geopandas as gpd
    import shapely

    if "bbox" in table.column_names:
        table = table.drop(["bbox"])
    df = table.drop(["geometry"]).to_pandas()
    geometry = shapely.from_wkb(table.column("geometry").to_numpy(zero_copy_only=False))
    return gpd.GeoDataFrame(df, geometry=geometry, crs=4326)


def read_lakes(hylak_ids, src: str | None = None):
    """
    Read the given lakes as a GeoDataFrame (EPSG:4326), in ``Hylak_id`` order.

    Missing IDs are simply absent from the result.
    """
    import pyarrow.parquet as pq

    ids = sorted({int(i) for i in hylak_ids})
    fs, path = _filesystem(src or source())
    columns = [n for n in pq.read_schema(path, filesystem=fs).names if n != "bbox"]
    table = pq.read_table(
        path, filesystem=fs, columns=columns, filters=[("Hylak_id", "in", ids)]
    )
    return _to_gdf(table)


def _row_group_for(pf, hylak_id: int) -> int | None:
    """
    The row group holding ``hylak_id`` in a file sorted by ID (``ROW_GROUP_SIZE``
    rows each, IDs 1, 2, 3...), checked against the Parquet statistics.
    None if the file is not laid out that way.
    """
    rg = (hylak_id - 1) // ROW_GROUP_SIZE
    if not 0 <= rg < pf.num_row_groups:
        return None
    meta = pf.metadata.row_group(rg)
    col = pf.schema_arrow.get_field_index("Hylak_id")
    stats = meta.column(col).statistics
    if stats is None or not stats.has_min_max:
        return None
    return rg if stats.min <= hylak_id <= stats.max else None


@functools.lru_cache(maxsize=64)
def _get_lake_cached(hylak_id: int, src: str):
    import pyarrow.compute as pc

    pf = _parquet_file(src)
    rg = _row_group_for(pf, hylak_id)
    if rg is None:  # not the standard layout: let Parquet filters find it
        gdf = read_lakes([hylak_id], src)
    else:
        with _READ_LOCK:  # one open file shared by every page's threads
            table = pf.read_row_group(rg)
        table = table.filter(pc.equal(table.column("Hylak_id"), hylak_id))
        gdf = _to_gdf(table)
    return None if gdf.empty else gdf


def get_lake(hylak_id: int, src: str | None = None):
    """
    One lake as a one-row GeoDataFrame, or None if the ID does not exist.

    Reads just the one row group that holds the ID; recent lakes are cached.
    """
    gdf = _get_lake_cached(int(hylak_id), src or source())
    return None if gdf is None else gdf.copy()


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


# ---------------------------------------------------------------- search index
#: Columns kept in memory for search (about 15 MB for all 1.4 million lakes).
INDEX_COLUMNS = ["Hylak_id", "Lake_name", "Country", "Lake_area"]
#: For each country, how many of its largest lakes the index keeps for search.
TOP_PER_COUNTRY = 50

_INDEX: dict = {}
_INDEX_LOCK = threading.Lock()


def normalize(text) -> str:
    """Lower case, accents removed, single spaces: 'Lac Léman' -> 'lac leman'."""
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.replace("_", " ").lower().split())


class LakeIndex:
    """
    Every lake's ID, name, country and area, for instant search and lookup.

    Built once per source from four small columns of the HydroLAKES file.
    Only ~3,000 lakes have a name; those are kept in a small table. For the
    rest, only IDs, areas and country codes are kept as numpy arrays.
    """

    def __init__(self, table):
        import pandas as pd

        df = table.to_pandas()
        df = df.sort_values("Hylak_id", ignore_index=True)
        self.ids = df["Hylak_id"].to_numpy(dtype=np.int64)
        self.area = df["Lake_area"].to_numpy(dtype=np.float64)
        country = df["Country"].astype("category")
        self.countries = list(country.cat.categories)
        self.country_code = country.cat.codes.to_numpy()
        self.country_keys = [normalize(c) for c in self.countries]

        named = df[df["Lake_name"].notna() & (df["Lake_name"].astype(str) != "")]
        named = named.assign(key=named["Lake_name"].map(normalize))
        self.named = named.sort_values("Lake_area", ascending=False, ignore_index=True)
        self.names = dict(zip(named["Hylak_id"], named["Lake_name"]))

        # The largest lakes of each country, biggest first (row positions).
        order = np.argsort(-np.nan_to_num(self.area, nan=-1.0), kind="stable")
        by_country = pd.Series(order).groupby(self.country_code[order])
        self.top_by_country = {
            int(code): grp.to_numpy()[:TOP_PER_COUNTRY]
            for code, grp in by_country
            if code >= 0
        }

    def _row(self, pos: int) -> dict:
        hylak_id = int(self.ids[pos])
        code = int(self.country_code[pos])
        area = float(self.area[pos])
        return {
            "hylak_id": hylak_id,
            "name": self.names.get(hylak_id),
            "country": self.countries[code] if code >= 0 else None,
            "area_km2": None if np.isnan(area) else area,
        }

    def info(self, hylak_id: int) -> dict | None:
        """``{hylak_id, name, country, area_km2}`` for one ID, or None."""
        pos = int(np.searchsorted(self.ids, int(hylak_id)))
        if pos < len(self.ids) and self.ids[pos] == int(hylak_id):
            return self._row(pos)
        return None

    def search(self, text: str, limit: int = 8) -> list[dict]:
        """
        Lakes matching ``text``, best first.

        * digits: the lake with that HYLAK_ID;
        * otherwise names starting with the text, then names containing it,
          then the largest lakes of matching countries (larger lakes first
          within each group). Case and accents are ignored.
        """
        text = str(text or "").strip().lstrip("#")
        if not text:
            return []
        if text.isdigit():
            hit = self.info(int(text))
            return [hit] if hit else []
        key = normalize(text)
        keys = self.named["key"]
        prefix = keys.str.startswith(key)
        words = keys.str.contains(" " + key, regex=False) & ~prefix
        inside = keys.str.contains(key, regex=False) & ~prefix & ~words
        ids = []
        for mask in (prefix, words, inside):
            ids += self.named.loc[mask, "Hylak_id"].tolist()
        out = [self.info(i) for i in ids[:limit]]
        if len(out) < limit:
            seen = {r["hylak_id"] for r in out}
            for code, ckey in enumerate(self.country_keys):
                if not ckey.startswith(key):
                    continue
                for pos in self.top_by_country.get(code, []):
                    if len(out) >= limit:
                        break
                    if int(self.ids[pos]) not in seen:
                        out.append(self._row(int(pos)))
                        seen.add(int(self.ids[pos]))
        return out[:limit]


def lake_index(src: str | None = None) -> LakeIndex:
    """The search index for a source, loaded on first use (a few seconds from S3)."""
    src = src or source()
    with _INDEX_LOCK:
        if src not in _INDEX:
            with _READ_LOCK:
                table = _parquet_file(src).read(columns=INDEX_COLUMNS)
            _INDEX[src] = LakeIndex(table)
        return _INDEX[src]


def index_ready(src: str | None = None) -> bool:
    """True once :func:`lake_index` has been loaded for this source."""
    return (src or source()) in _INDEX


def search(text: str, limit: int = 8, src: str | None = None) -> list[dict]:
    """Search lakes by name, country or HYLAK_ID (see :meth:`LakeIndex.search`)."""
    return lake_index(src).search(text, limit)


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

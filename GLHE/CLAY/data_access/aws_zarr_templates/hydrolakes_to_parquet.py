"""
Convert the HydroLAKES shapefile into the GeoParquet file GLHE reads.

    python -m GLHE.CLAY.data_access.aws_zarr_templates.hydrolakes_to_parquet \\
        HydroLAKES_polys_v10.shp HydroLAKES_polys_v10.parquet

then upload it (needs write access to the bucket):

    aws s3 cp HydroLAKES_polys_v10.parquet s3://glhe/HydroLAKES/HydroLAKES_polys_v10.parquet

The shapefile is already sorted by Hylak_id, so it is streamed in chunks
(low memory) into row groups of ``hydrolakes.ROW_GROUP_SIZE`` rows. See
``GLHE/CLAY/hydrolakes.py`` for the layout and how it is read.
"""

import os
import sys

import numpy as np
import pyarrow.parquet as pq
import pyogrio

from GLHE.CLAY import hydrolakes

CHUNK = 50_000


def convert(src: str, dst: str) -> None:
    n = pyogrio.read_info(src)["features"]
    tmp = dst + ".tmp"
    writer, schema = None, None
    bounds = [np.inf, np.inf, -np.inf, -np.inf]
    types = set()
    last_id = 0
    for start in range(0, n, CHUNK):
        gdf = pyogrio.read_dataframe(src, skip_features=start, max_features=CHUNK)
        gdf = gdf.to_crs(4326)
        ids = gdf["Hylak_id"].to_numpy()
        if ids[0] <= last_id or np.any(np.diff(ids) <= 0):
            raise ValueError("expected the shapefile to be sorted by Hylak_id")
        last_id = ids[-1]
        b = gdf.total_bounds
        bounds = [
            min(bounds[0], b[0]),
            min(bounds[1], b[1]),
            max(bounds[2], b[2]),
            max(bounds[3], b[3]),
        ]
        types |= set(gdf.geom_type)
        table = hydrolakes.to_arrow(gdf)
        if writer is None:
            schema = table.schema
            writer = pq.ParquetWriter(
                tmp, schema, compression="zstd", compression_level=9
            )
        writer.write_table(table.cast(schema), row_group_size=hydrolakes.ROW_GROUP_SIZE)
        print(f"{min(start + CHUNK, n)}/{n}", flush=True)
    writer.close()

    # Rewrite once with the GeoParquet "geo" metadata (needs overall bounds/types).
    table = pq.read_table(tmp)
    table = table.replace_schema_metadata(
        {b"geo": hydrolakes.geo_metadata(bounds, types)}
    )
    pq.write_table(
        table,
        dst,
        compression="zstd",
        compression_level=9,
        row_group_size=hydrolakes.ROW_GROUP_SIZE,
    )
    os.remove(tmp)
    print(f"wrote {dst}: {os.path.getsize(dst) / 1e9:.2f} GB", flush=True)


if __name__ == "__main__":
    convert(sys.argv[1], sys.argv[2])

#!/usr/bin/env python
"""Convert one or more NetCDF files to a chunked, compressed Zarr store.

Designed for GLHE data products (global gridded monthly data).  Rechunks
for spatial subsetting — the dominant access pattern in GLHE — and compresses
with Blosc/zstd before writing.

Usage
-----
Single file:
    python netcdf_to_zarr.py --input /data/CRUTS.nc --output /data/zarr/CRUTS.zarr

Multi-year files (glob):
    python netcdf_to_zarr.py --input "/data/ERA5_*.nc" --output /data/zarr/ERA5.zarr

Custom chunking:
    python netcdf_to_zarr.py --input /data/GLEAM.nc --output /data/zarr/GLEAM.zarr \
        --time-chunks 12 --lat-chunks 72 --lon-chunks 72

Skip compression (faster write, larger file):
    python netcdf_to_zarr.py --input /data/data.nc --output /data/zarr/data.zarr \
        --no-compress

After conversion, verify with:
    python -c "import xarray as xr; print(xr.open_zarr('path/to.zarr'))"
"""

import argparse
import glob
import sys
from pathlib import Path

import xarray as xr
import zarr

# Dimension names that GLHE normalises to lat/lon.
_DIM_RENAMES = {
    "latitude": "lat",
    "longitude": "lon",
    "Latitude": "lat",
    "Longitude": "lon",
    "nav_lat": "lat",
    "nav_lon": "lon",
}


def _parse_args():
    p = argparse.ArgumentParser(
        description="Convert NetCDF to chunked Zarr for GLHE S3 hosting.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--input",
        required=True,
        help="Path or glob pattern for input NetCDF file(s).",
    )
    p.add_argument(
        "--output",
        required=True,
        help="Path for the output Zarr store (e.g. /data/zarr/CRUTS.zarr).",
    )
    p.add_argument(
        "--time-chunks",
        type=int,
        default=10,
        help="Chunk size along the time dimension (default: 10).",
    )
    p.add_argument(
        "--lat-chunks",
        type=int,
        default=100,
        help="Chunk size along the lat dimension (default: 100).",
    )
    p.add_argument(
        "--lon-chunks",
        type=int,
        default=100,
        help="Chunk size along the lon dimension (default: 100).",
    )
    p.add_argument(
        "--no-compress",
        action="store_true",
        help="Skip Blosc compression (faster write, larger output).",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite an existing Zarr store at --output.",
    )
    return p.parse_args()


def _resolve_inputs(pattern: str) -> list[str]:
    paths = sorted(glob.glob(pattern))
    if not paths:
        # Try interpreting as a literal path
        if Path(pattern).exists():
            return [pattern]
        print(f"ERROR: no files matched: {pattern}", file=sys.stderr)
        sys.exit(1)
    return paths


def _build_chunks(ds: xr.Dataset, time_chunks: int, lat_chunks: int, lon_chunks: int) -> dict:
    """Build a chunk dict that only names dimensions that actually exist."""
    dim_map = {}
    for dim in ds.dims:
        normalised = _DIM_RENAMES.get(dim, dim)
        if normalised == "time" or dim == "time":
            dim_map[dim] = time_chunks
        elif normalised == "lat":
            dim_map[dim] = lat_chunks
        elif normalised == "lon":
            dim_map[dim] = lon_chunks
    return dim_map


def convert(
    input_pattern: str,
    output_path: str,
    time_chunks: int = 10,
    lat_chunks: int = 100,
    lon_chunks: int = 100,
    compress: bool = True,
    overwrite: bool = False,
) -> None:
    out = Path(output_path)
    if out.exists() and not overwrite:
        print(f"ERROR: {out} already exists. Use --overwrite to replace it.")
        sys.exit(1)

    input_files = _resolve_inputs(input_pattern)
    print(f"Opening {len(input_files)} file(s): {[Path(f).name for f in input_files]}")

    ds = xr.open_mfdataset(input_files, combine="by_coords", chunks="auto")

    # Normalise dimension names so GLHE helpers find lat/lon reliably.
    rename = {k: v for k, v in _DIM_RENAMES.items() if k in ds.dims}
    if rename:
        print(f"Renaming dimensions: {rename}")
        ds = ds.rename(rename)

    chunks = _build_chunks(ds, time_chunks, lat_chunks, lon_chunks)
    print(f"Rechunking to: {chunks}")
    ds = ds.chunk(chunks)

    encoding = {}
    if compress:
        compressor = zarr.Blosc(cname="zstd", clevel=3, shuffle=zarr.Blosc.BITSHUFFLE)
        encoding = {var: {"compressor": compressor} for var in ds.data_vars}
        print("Compression: Blosc/zstd clevel=3 bitshuffle")
    else:
        print("Compression: none (--no-compress)")

    print(f"Writing Zarr to: {out} ...")
    write_mode = "w" if overwrite else "w-"
    ds.to_zarr(str(out), encoding=encoding, consolidated=True, mode=write_mode)

    # Report size
    total_bytes = sum(f.stat().st_size for f in out.rglob("*") if f.is_file())
    print(f"Done. Zarr store size: {total_bytes / 1e9:.2f} GB")
    print(f"Variables: {list(ds.data_vars)}")
    print(f"Time range: {ds.time.values[0]} → {ds.time.values[-1]}")
    print(f"Spatial extent: lat [{float(ds.lat.min()):.2f}, {float(ds.lat.max()):.2f}]  "
          f"lon [{float(ds.lon.min()):.2f}, {float(ds.lon.max()):.2f}]")


if __name__ == "__main__":
    args = _parse_args()
    convert(
        input_pattern=args.input,
        output_path=args.output,
        time_chunks=args.time_chunks,
        lat_chunks=args.lat_chunks,
        lon_chunks=args.lon_chunks,
        compress=not args.no_compress,
        overwrite=args.overwrite,
    )

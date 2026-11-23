#!/usr/bin/env python
"""Upload a local Zarr store to an S3 bucket.

Syncs the Zarr directory tree file-by-file using s3fs.  Does not re-read the
data through xarray — files are copied as-is, preserving chunk layout and
compression metadata exactly.

Usage
-----
Upload to the GLHE public bucket:
    python zarr_to_aws.py --zarr /data/zarr/CRUTS.zarr \
        --bucket glhe --key zarr/CRUTS.zarr

Dry run (list what would be uploaded without uploading):
    python zarr_to_aws.py --zarr /data/zarr/CRUTS.zarr \
        --bucket glhe --key zarr/CRUTS.zarr --dry-run

Use a named AWS profile (e.g. from ~/.aws/credentials):
    python zarr_to_aws.py --zarr /data/zarr/CRUTS.zarr \
        --bucket glhe --key zarr/CRUTS.zarr --profile prod

After uploading, verify the store is readable:
    python -c "
    import xarray as xr, s3fs
    fs = s3fs.S3FileSystem(anon=True)
    ds = xr.open_zarr(fs.get_mapper('s3://glhe/zarr/CRUTS.zarr'))
    print(ds)
    "

Notes
-----
- AWS credentials must be configured (aws configure, IAM role, or --profile).
- For the public glhe bucket, anonymous read works but authenticated write is required.
- After a fresh upload, GLHE's data_check.py can verify the store is reachable.
"""

import argparse
import sys
from pathlib import Path

import s3fs


def _parse_args():
    p = argparse.ArgumentParser(
        description="Upload a local Zarr store to S3.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--zarr",
        required=True,
        help="Path to the local Zarr store directory.",
    )
    p.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket name (e.g. 'glhe').",
    )
    p.add_argument(
        "--key",
        required=True,
        help="S3 key prefix for the Zarr store (e.g. 'zarr/CRUTS.zarr').",
    )
    p.add_argument(
        "--profile",
        default=None,
        help="AWS profile name from ~/.aws/credentials (optional).",
    )
    p.add_argument(
        "--region",
        default=None,
        help="AWS region (optional; uses your default region if omitted).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be uploaded without actually uploading.",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing objects at the destination (default: skip existing).",
    )
    return p.parse_args()


def upload(
    zarr_path: str,
    bucket: str,
    s3_key: str,
    profile: str | None = None,
    region: str | None = None,
    dry_run: bool = False,
    overwrite: bool = False,
) -> None:
    local_root = Path(zarr_path)
    if not local_root.exists():
        print(f"ERROR: {local_root} does not exist.", file=sys.stderr)
        sys.exit(1)

    s3_prefix = f"{bucket}/{s3_key.lstrip('/')}"

    # Collect files and compute sizes
    local_files = sorted(f for f in local_root.rglob("*") if f.is_file())
    if not local_files:
        print(f"ERROR: no files found in {local_root}", file=sys.stderr)
        sys.exit(1)

    total_bytes = sum(f.stat().st_size for f in local_files)
    print(f"Zarr store: {local_root}")
    print(f"Destination: s3://{s3_prefix}")
    print(f"Files: {len(local_files)}   Size: {total_bytes / 1e6:.1f} MB")

    if dry_run:
        print("\n--- DRY RUN: files that would be uploaded ---")
        for f in local_files:
            rel = f.relative_to(local_root)
            print(f"  s3://{s3_prefix}/{rel}  ({f.stat().st_size:,} bytes)")
        print("--- end dry run ---")
        return

    # Authenticate
    kwargs = {}
    if profile:
        kwargs["profile"] = profile
    if region:
        kwargs["client_kwargs"] = {"region_name": region}
    fs = s3fs.S3FileSystem(**kwargs)

    uploaded = 0
    skipped = 0
    for local_file in local_files:
        rel = local_file.relative_to(local_root)
        s3_dest = f"{s3_prefix}/{rel}"

        if not overwrite and fs.exists(s3_dest):
            skipped += 1
            continue

        fs.put(str(local_file), s3_dest)
        uploaded += 1
        if uploaded % 50 == 0 or uploaded == 1:
            print(f"  uploaded {uploaded}/{len(local_files) - skipped} files ...")

    print(f"\nDone.  Uploaded: {uploaded}  Skipped (already exists): {skipped}")
    print(f"Store available at: s3://{s3_prefix}")


if __name__ == "__main__":
    args = _parse_args()
    upload(
        zarr_path=args.zarr,
        bucket=args.bucket,
        s3_key=args.key,
        profile=args.profile,
        region=args.region,
        dry_run=args.dry_run,
        overwrite=args.overwrite,
    )

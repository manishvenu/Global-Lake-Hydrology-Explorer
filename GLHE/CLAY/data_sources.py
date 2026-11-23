"""
Where CLAY reads its input data from.

Every input has one *relative path*, the same on the GLHE data server and in
the S3 bucket::

    HydroLAKES/HydroLAKES_polys_v10.parquet
    zarr/ERA5.zarr
    zarr/CRUTS.zarr

They are tried in this order:

1. **this machine**: the copy under ``GLHE/CLAY/LocalData/`` (each caller
   says where; e.g. ``LocalData/zarr/ERA5.zarr``);
2. **the GLHE data server**: ``https://glhe.venumuddula.com/<relative path>``
   (set ``GLHE_DATA_URL`` to use another server, or to ``""`` to skip it);
3. **S3**: ``s3://glhe/<relative path>``, read anonymously. Kept as a fallback
   in case the data is put back on S3.

On the server itself, step 1 always wins, so it never downloads from itself.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

logger = logging.getLogger(__name__)

SERVER_ENV = "GLHE_DATA_URL"
DEFAULT_SERVER = "https://glhe.venumuddula.com"
S3_BUCKET = "glhe"

#: How long (seconds) to remember whether the server has a file.
CHECK_TTL = 300
_checked: dict[str, tuple[bool, float]] = {}


def server_url(relative: str) -> str | None:
    """``<server>/<relative>``, or None if no server is configured."""
    base = os.environ.get(SERVER_ENV, DEFAULT_SERVER).strip().rstrip("/")
    return f"{base}/{relative}" if base else None


def s3_url(relative: str) -> str:
    return f"s3://{S3_BUCKET}/{relative}"


def on_server(relative: str) -> bool:
    """True if the data server answers for this file (a Zarr store's
    ``.zmetadata``). Remembered for a few minutes either way."""
    url = server_url(relative)
    if url is None:
        return False
    hit = _checked.get(url)
    if hit and time.monotonic() - hit[1] < CHECK_TTL:
        return hit[0]
    import requests

    probe = url + "/.zmetadata" if relative.endswith(".zarr") else url
    try:
        ok = requests.head(probe, timeout=5, allow_redirects=True).status_code == 200
    except requests.RequestException:
        ok = False
    _checked[url] = (ok, time.monotonic())
    return ok


def open_zarr(relative: str | None, local_path: Path | str | None):
    """
    Open a Zarr store: the local copy, else the data server, else S3.

    ``relative`` is the path on the server and in S3 (None: local only).
    Raises FileNotFoundError, listing what was tried, if none works.
    """
    import xarray as xr

    if local_path is not None and Path(local_path).exists():
        logger.info("Reading %s", local_path)
        return xr.open_dataset(str(local_path), engine="zarr")
    tried = [f"{local_path} (missing)"] if local_path is not None else []
    if relative:
        url = server_url(relative)
        if url:
            try:
                ds = xr.open_dataset(url, engine="zarr", consolidated=True)
                logger.info("Reading %s", url)
                return ds
            except Exception as exc:  # server down, file not there...
                tried.append(f"{url} ({type(exc).__name__})")
        try:
            ds = _open_s3_zarr(s3_url(relative))
            logger.info("Reading %s", s3_url(relative))
            return ds
        except Exception as exc:
            tried.append(f"{s3_url(relative)} ({type(exc).__name__})")
    raise FileNotFoundError("Could not open " + "; ".join(tried))


def _open_s3_zarr(url: str):
    import xarray as xr

    return xr.open_dataset(
        url, engine="zarr", backend_kwargs={"storage_options": {"anon": True}}
    )

"""Reading CLAY inputs from this machine, the GLHE data server, or S3.

A small web server on localhost stands in for the data server, so these
tests run offline but really go over HTTP (including byte ranges, which the
Parquet reader needs)."""

import http.server
import os
import threading
from functools import partial

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely.geometry import box

from GLHE.CLAY import data_sources, hydrolakes


class RangeHandler(http.server.SimpleHTTPRequestHandler):
    """Static files with HTTP Range support (like the real server)."""

    def log_message(self, *args):
        pass

    def send_head(self):
        rng = self.headers.get("Range")
        path = self.translate_path(self.path)
        if not rng or not os.path.isfile(path):
            return super().send_head()
        size = os.path.getsize(path)
        start, _, end = rng.removeprefix("bytes=").partition("-")
        start, end = int(start or 0), min(int(end) if end else size - 1, size - 1)
        f = open(path, "rb")
        f.seek(start)
        self.send_response(206)
        self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Content-Length", str(end - start + 1))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        self._remaining = end - start + 1
        return f

    def copyfile(self, source, outputfile):
        n = getattr(self, "_remaining", None)
        outputfile.write(source.read(n) if n is not None else source.read())


@pytest.fixture
def data_server(tmp_path, monkeypatch):
    """A local 'GLHE data server' serving tmp_path/served; returns that folder."""
    root = tmp_path / "served"
    root.mkdir()
    server = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0), partial(RangeHandler, directory=str(root))
    )
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv(
        data_sources.SERVER_ENV, f"http://127.0.0.1:{server.server_port}"
    )
    data_sources._checked.clear()
    yield root
    server.shutdown()
    data_sources._checked.clear()


@pytest.fixture
def no_s3(monkeypatch):
    """Fail loudly if S3 is tried."""

    def refuse(url):
        raise RuntimeError(f"S3 should not be used: {url}")

    monkeypatch.setattr(data_sources, "_open_s3_zarr", refuse)


def small_zarr(path):
    time = pd.date_range("2000-01-01", periods=3, freq="MS")
    xr.Dataset(
        {
            "pre": (
                ("time", "lat", "lon"),
                np.full((3, 2, 2), 5.0),
                {"units": "mm/month"},
            )
        },
        coords={"time": time, "lat": [40.0, 40.5], "lon": [-112.0, -111.5]},
    ).to_zarr(path, mode="w", consolidated=True, zarr_format=2)


# ------------------------------------------------------------------ urls
def test_server_url_default_override_and_off(monkeypatch):
    monkeypatch.delenv(data_sources.SERVER_ENV, raising=False)
    assert data_sources.server_url("zarr/ERA5.zarr") == (
        "https://glhe.venumuddula.com/zarr/ERA5.zarr"
    )
    monkeypatch.setenv(data_sources.SERVER_ENV, "http://example.org/")
    assert data_sources.server_url("a/b") == "http://example.org/a/b"
    monkeypatch.setenv(data_sources.SERVER_ENV, "")
    assert data_sources.server_url("a/b") is None
    assert data_sources.on_server("a/b") is False
    assert data_sources.s3_url("zarr/CRUTS.zarr") == "s3://glhe/zarr/CRUTS.zarr"


# ------------------------------------------------------------------ zarr
def test_local_copy_wins(tmp_path, data_server, no_s3):
    small_zarr(tmp_path / "local.zarr")
    ds = data_sources.open_zarr("zarr/X.zarr", tmp_path / "local.zarr")
    assert float(ds["pre"].mean()) == 5.0


def test_server_used_when_no_local_copy(tmp_path, data_server, no_s3):
    small_zarr(data_server / "zarr" / "X.zarr")
    assert data_sources.on_server("zarr/X.zarr")
    ds = data_sources.open_zarr("zarr/X.zarr", tmp_path / "missing.zarr")
    assert ds["pre"].shape == (3, 2, 2)
    assert float(ds["pre"].mean()) == 5.0


def test_s3_used_when_server_lacks_it(tmp_path, data_server, monkeypatch):
    opened = []
    monkeypatch.setattr(
        data_sources, "_open_s3_zarr", lambda url: opened.append(url) or "S3"
    )
    assert not data_sources.on_server("zarr/Nope.zarr")
    assert data_sources.open_zarr("zarr/Nope.zarr", tmp_path / "missing.zarr") == "S3"
    assert opened == ["s3://glhe/zarr/Nope.zarr"]


def test_nothing_anywhere_raises(tmp_path, data_server, no_s3):
    with pytest.raises(FileNotFoundError, match="missing"):
        data_sources.open_zarr("zarr/Nope.zarr", tmp_path / "missing.zarr")


# ------------------------------------------------------------- HydroLAKES
def test_hydrolakes_read_from_the_server(tmp_path, data_server, monkeypatch):
    import geopandas as gpd

    gdf = gpd.GeoDataFrame(
        {
            "Hylak_id": [67, 68],
            "Lake_name": ["Great Salt Lake", None],
            "Country": ["United States", "United States"],
            "Lake_area": [4360.0, 1.5],
        },
        geometry=[box(-112.9, 40.7, -112.0, 41.6), box(-111.0, 40.0, -110.99, 40.01)],
        crs=4326,
    )
    (data_server / "HydroLAKES").mkdir()
    hydrolakes.write_parquet(gdf, data_server / hydrolakes.RELATIVE_PATH)
    monkeypatch.delenv(hydrolakes.ENV_VAR, raising=False)
    monkeypatch.setattr(hydrolakes, "LOCAL_PATH", tmp_path / "missing.parquet")

    src = hydrolakes.source()
    assert src == data_sources.server_url(hydrolakes.RELATIVE_PATH)
    assert hydrolakes.available(src)
    lake = hydrolakes.get_lake(67, src)
    assert lake["Lake_name"].iloc[0] == "Great Salt Lake"
    assert hydrolakes.search("great", src=src)[0]["hylak_id"] == 67


def test_hydrolakes_falls_back_to_s3(tmp_path, data_server, monkeypatch):
    monkeypatch.delenv(hydrolakes.ENV_VAR, raising=False)
    monkeypatch.setattr(hydrolakes, "LOCAL_PATH", tmp_path / "missing.parquet")
    assert hydrolakes.source() == "s3://glhe/" + hydrolakes.RELATIVE_PATH

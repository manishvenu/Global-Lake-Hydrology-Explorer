"""Product plumbing: missing data and broken products are skipped, and
spec-driven gridded products work (all offline)."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely.geometry import box

from GLHE.CLAY import ureg
from GLHE.CLAY import CLAY_driver, helpers
from GLHE.CLAY.data_access import data_check, gridded_product

LAKE = box(-112.4, 40.6, -111.6, 41.4)  # a lake inside the test grid


@pytest.fixture(autouse=True)
def logging_dir(tmp_path):
    helpers.setup_logging_directory(str(tmp_path / "logs"))


@pytest.fixture
def local_data(tmp_path, monkeypatch):
    """An empty LocalData folder for this test."""
    folder = tmp_path / "LocalData"
    folder.mkdir()
    monkeypatch.setattr(data_check, "_LOCAL_DATA_DIR", folder)
    return folder


def write_grid(path, lon_0_360=False, var="precipitation", units="mm/day"):
    """A small 0.5-degree grid with 1 mm/day everywhere, for 2000."""
    lon = np.arange(-114.0, -109.9, 0.5)
    if lon_0_360:
        lon = lon % 360
    time = pd.date_range("2000-01-01", periods=12, freq="MS")
    data = np.ones((len(time), 9, len(lon)), dtype="float32")
    ds = xr.Dataset(
        {var: (("time", "latitude", "longitude"), data, {"units": units})},
        coords={"time": time, "latitude": np.arange(39.0, 43.1, 0.5), "longitude": lon},
    )
    ds.to_zarr(path, mode="w")


def spec(**extra):
    base = {
        "product_name": "TestRain",
        "local_path": "zarr/test.zarr",
        "variables": {"precipitation": "p"},
    }
    base.update(extra)
    return base


# ----------------------------------------------------------- data_check
def test_data_check_skips_missing_cloud_copy(monkeypatch, local_data):
    monkeypatch.setattr(
        data_check,
        "_load_products",
        lambda: [
            {
                "name": "X",
                "access_code": "local",
                "local_path": "x.zarr",
                "cloud": "zarr/x.zarr",
            },
            {"name": "Y", "access_code": "api", "api_access_script": "Does_Not_Exist"},
        ],
    )
    monkeypatch.setattr(data_check, "s3_object_or_folder_exists", lambda b, p: False)
    data_check.check_data_and_download_missing_data_or_files()  # no RuntimeError


def test_data_check_survives_s3_being_unreachable(monkeypatch, local_data):
    monkeypatch.setattr(
        data_check,
        "_load_products",
        lambda: [
            {
                "name": "X",
                "access_code": "local",
                "local_path": "x.zarr",
                "cloud": "zarr/x.zarr",
            }
        ],
    )

    def offline(bucket, path):
        raise ConnectionError("no network")

    monkeypatch.setattr(data_check, "s3_object_or_folder_exists", offline)
    data_check.check_data_and_download_missing_data_or_files()


# ---------------------------------------------------------------- driver
def test_driver_skips_products_that_cannot_be_set_up(local_data):
    driver = CLAY_driver.CLAY_driver()
    products = driver.load_data_product_list(
        {
            "NoSuchModule": {
                "module_name": "nope",
                "class_name": "Nope",
                "run_on_start": True,
            },
            "BadSpec": {
                "module_name": "gridded_product",
                "class_name": "GriddedProduct",
                "run_on_start": True,
                "spec": {
                    "product_name": "Bad",
                    "local_path": "x",
                    "variables": {"q": "i"},
                },
            },
            "Good": {
                "module_name": "gridded_product",
                "class_name": "GriddedProduct",
                "run_on_start": True,
                "spec": spec(),
            },
        }
    )
    assert products["NoSuchModule"]["object"] is None
    assert products["BadSpec"]["object"] is None  # inflow is not supported
    assert isinstance(products["Good"]["object"], gridded_product.GriddedProduct)


def test_default_products_still_load():
    products = CLAY_driver.CLAY_driver().load_data_product_list()
    assert "CRUTS" in products and products["CRUTS"]["object"] is not None


# ------------------------------------------------------- gridded product
def test_gridded_product_reads_converts_and_averages(local_data):
    write_grid(local_data / "zarr" / "test.zarr")
    product = gridded_product.GriddedProduct(spec())
    (series,) = product.product_driver(LAKE)
    assert series.single_letter_code == "p"
    assert series.product_name == "TestRain"
    assert str(series.unit) == str(ureg.parse_units("mm/month"))
    # 1 mm/day for a (pint) month of 30.44 days
    assert series.dataset.iloc[0] == pytest.approx(30.44, rel=0.01)
    assert series.dataset.index[0] == pd.Timestamp("2000-01-01")
    assert list(series.xarray_dataarray.dims) == ["time", "lat", "lon"]


def test_gridded_product_handles_0_360_longitudes_and_unit_override(local_data):
    write_grid(local_data / "zarr" / "test.zarr", lon_0_360=True, units="weird")
    product = gridded_product.GriddedProduct(spec(units={"precipitation": "mm/day"}))
    (series,) = product.product_driver(LAKE)
    assert series.dataset.iloc[0] == pytest.approx(30.44, rel=0.01)


def test_gridded_product_reads_netcdf_files(local_data):
    folder = local_data / "nc"
    folder.mkdir()
    write_grid(local_data / "tmp.zarr")
    xr.open_zarr(local_data / "tmp.zarr").to_netcdf(folder / "rain_2000.nc")
    product = gridded_product.GriddedProduct(spec(local_path="nc/*.nc"))
    (series,) = product.product_driver(LAKE)
    assert len(series.dataset) == 12


def test_gridded_product_without_data_raises(local_data):
    product = gridded_product.GriddedProduct(spec())
    with pytest.raises(FileNotFoundError):
        product.product_driver(LAKE)

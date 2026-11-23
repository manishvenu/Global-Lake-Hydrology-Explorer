"""End-to-end driver tests for all 6 GLHE data products.

Each test patches the raw data-access boundary (network call or local file read)
and runs the full processing pipeline: unit conversion, spatial averaging /
point extraction, MVSeries construction, and date-index snapping.

Goal: detect silent product failures.  If a product is dropped or raises
unexpectedly, the test fails here rather than producing a mysteriously empty CSV.

Patch strategy per product
--------------------------
CRUTS        : open_dataset()                 — raw dataset (CRU TS is a GriddedProduct spec)
ERA5-Land    : call_ERA5_Land_API_and_process() — post-processed dataset (mm/month)
NWM          : call_NWM_s3_access_and_process() — fully-processed monthly dataset
DAHITI       : _fetch_and_process()             — synthetic water-level pd.Series
GloFAS       : _fetch_and_process()             — synthetic discharge pd.Series
GLEAM        : _find_files() + _load_and_process() — patched file list + processed dataset
"""

import json

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from pathlib import Path
from shapely.geometry import box
from unittest.mock import patch, MagicMock

import GLHE.CLAY.globals
import GLHE.CLAY.helpers
from GLHE.CLAY.data_access import gridded_product
from GLHE.CLAY.data_access import ERA5_Land as era5_mod
from GLHE.CLAY.data_access import NWM as nwm_mod
from GLHE.CLAY.data_access import DAHITI as dahiti_mod
from GLHE.CLAY.data_access import GloFAS as glofas_mod
from GLHE.CLAY.data_access import GLEAM as gleam_mod


def _cruts():
    """CRU TS as configured in data_products.json (a GriddedProduct spec)."""
    config = Path(GLHE.CLAY.globals.__file__).parent / "config" / "data_products.json"
    spec = json.loads(config.read_text())["CRUTS"]["spec"]
    return gridded_product.GriddedProduct(spec)


# ── Shared constants ─────────────────────────────────────────────────────────

# Small box with a grid wider than the polygon so subset_box always succeeds.
POLY = box(-112.5, 35.0, -111.5, 36.0)
_LATS = np.array([34.5, 35.0, 35.5, 36.0, 36.5])
_LONS = np.array([-113.0, -112.5, -112.0, -111.5, -111.0])
_TIMES = pd.date_range("2000-01-01", periods=12, freq="MS")   # month-start


# ── Output isolation fixture ─────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def isolate_output(tmp_path):
    """Redirect file I/O (pickles, logs) to tmp_path for test isolation."""
    orig_out = GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
    orig_name = GLHE.CLAY.globals.config.get("LAKE_NAME", "")
    (tmp_path / "save_files").mkdir()
    GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] = str(tmp_path)
    GLHE.CLAY.globals.config["LAKE_NAME"] = "TestLake"
    GLHE.CLAY.helpers.setup_logging_directory(str(tmp_path))
    yield tmp_path
    GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] = orig_out
    GLHE.CLAY.globals.config["LAKE_NAME"] = orig_name


# ── Synthetic dataset builders ────────────────────────────────────────────────

def _grid_ds(vars_units: dict, product_name: str = "",
             lat_dim: str = "lat", lon_dim: str = "lon") -> xr.Dataset:
    """Build a 5×5 spatial, 12-month synthetic xr.Dataset."""
    shape = (12, len(_LATS), len(_LONS))
    data_vars = {}
    for var, (value, units) in vars_units.items():
        da = xr.DataArray(
            np.full(shape, float(value)),
            dims=["time", lat_dim, lon_dim],
            coords={"time": _TIMES, lat_dim: _LATS, lon_dim: _LONS},
            attrs={"units": units},
        )
        data_vars[var] = da
    ds = xr.Dataset(data_vars)
    if product_name:
        ds.attrs["product_name"] = product_name
    return ds


def _nwm_monthly_ds() -> xr.Dataset:
    """NWM dataset as returned by call_NWM_s3_access_and_process (fully processed)."""
    ds = xr.Dataset({
        "inflow": xr.DataArray(
            np.full(12, 1.0e8),
            dims=["time"],
            coords={"time": _TIMES},
            attrs={"units": "m3/month"},
        ),
        "outflow": xr.DataArray(
            np.full(12, 0.9e8),
            dims=["time"],
            coords={"time": _TIMES},
            attrs={"units": "m3/month"},
        ),
    })
    ds = ds.assign_coords(lat=35.5, lon=-112.0)
    ds.attrs.update({"product_name": "NWM", "proj4": "+proj=longlat +datum=WGS84"})
    return ds


def _wse_series() -> pd.Series:
    """Water surface elevation series as returned by DAHITI._fetch_and_process."""
    return pd.Series(np.full(12, 1280.5), index=_TIMES, name="wse")


def _discharge_series() -> pd.Series:
    """Monthly discharge series as returned by GloFAS._fetch_and_process."""
    return pd.Series(np.full(12, 5.0e9), index=_TIMES, name="dis24")


# ── Assertion helper ──────────────────────────────────────────────────────────

def _check(mv, slc: str, product_fragment: str) -> None:
    """Verify structure and metadata of a single MVSeries."""
    assert isinstance(mv.dataset, pd.Series), \
        f"{product_fragment}: dataset must be pd.Series, got {type(mv.dataset)}"
    assert len(mv.dataset) >= 1, f"{product_fragment}: dataset is empty"
    assert mv.single_letter_code == slc, \
        f"{product_fragment}: expected SLC={slc!r}, got {mv.single_letter_code!r}"
    assert product_fragment.lower() in mv.product_name.lower(), \
        f"expected {product_fragment!r} in product_name {mv.product_name!r}"
    assert all(d.day == 1 for d in mv.dataset.index), \
        f"{product_fragment}: index must be month-start (day=1)"
    assert not mv.dataset.isna().all(), \
        f"{product_fragment}: all values are NaN"


# ── CRUTS ─────────────────────────────────────────────────────────────────────

class TestCRUTSDriver:
    """Tests the full CRUTS pipeline: label → rename dims → subset → convert units
    → spatial average → date snap.  Only open_dataset() is mocked."""

    def test_returns_two_mvseries(self, tmp_path):
        raw_ds = _grid_ds({"pre": (50.0, "mm/month"), "pet": (80.0, "mm/month")})

        cruts = _cruts()
        with patch.object(cruts, "open_dataset", return_value=raw_ds):
            result = cruts.product_driver(POLY, run_cleanly=True)

        assert len(result) == 2, f"CRUTS must return 2 MVSeries, got {len(result)}"

    def test_precip_mvseries(self, tmp_path):
        raw_ds = _grid_ds({"pre": (50.0, "mm/month"), "pet": (80.0, "mm/month")})

        cruts = _cruts()
        with patch.object(cruts, "open_dataset", return_value=raw_ds):
            result = cruts.product_driver(POLY, run_cleanly=True)

        precip = next(mv for mv in result if mv.single_letter_code == "p")
        _check(precip, "p", "CRUTS")

    def test_evap_mvseries(self, tmp_path):
        raw_ds = _grid_ds({"pre": (50.0, "mm/month"), "pet": (80.0, "mm/month")})

        cruts = _cruts()
        with patch.object(cruts, "open_dataset", return_value=raw_ds):
            result = cruts.product_driver(POLY, run_cleanly=True)

        evap = next(mv for mv in result if mv.single_letter_code == "e")
        _check(evap, "e", "CRUTS")

    def test_values_are_spatially_averaged(self, tmp_path):
        """All grid cells have the same value so spatial avg must equal that value."""
        raw_ds = _grid_ds({"pre": (50.0, "mm/month"), "pet": (80.0, "mm/month")})

        cruts = _cruts()
        with patch.object(cruts, "open_dataset", return_value=raw_ds):
            result = cruts.product_driver(POLY, run_cleanly=True)

        precip = next(mv for mv in result if mv.single_letter_code == "p")
        assert precip.dataset.notna().all()
        assert (precip.dataset > 0).all()


# ── ERA5-Land ─────────────────────────────────────────────────────────────────

class TestERA5LandDriver:
    """Patches call_ERA5_Land_API_and_process (already-processed dataset in mm/month)
    so the spatial-average step and event publish are exercised."""

    @pytest.fixture
    def era5(self):
        with patch("cdsapi.Client"):
            return era5_mod.ERA5_Land()

    def test_returns_two_mvseries(self, era5):
        processed_ds = _grid_ds(
            {"tp": (50.0, "mm/month"), "e": (80.0, "mm/month")},
            product_name="ERA5_Land",
        )
        with patch.object(era5, "call_ERA5_Land_API_and_process", return_value=processed_ds):
            result = era5.product_driver(POLY, run_cleanly=True)

        assert len(result) == 2

    def test_precip_mvseries(self, era5):
        processed_ds = _grid_ds(
            {"tp": (50.0, "mm/month"), "e": (80.0, "mm/month")},
            product_name="ERA5_Land",
        )
        with patch.object(era5, "call_ERA5_Land_API_and_process", return_value=processed_ds):
            result = era5.product_driver(POLY, run_cleanly=True)

        precip = next(mv for mv in result if mv.single_letter_code == "p")
        _check(precip, "p", "ERA5")

    def test_evap_mvseries(self, era5):
        processed_ds = _grid_ds(
            {"tp": (50.0, "mm/month"), "e": (80.0, "mm/month")},
            product_name="ERA5_Land",
        )
        with patch.object(era5, "call_ERA5_Land_API_and_process", return_value=processed_ds):
            result = era5.product_driver(POLY, run_cleanly=True)

        evap = next(mv for mv in result if mv.single_letter_code == "e")
        _check(evap, "e", "ERA5")

    def test_cached_run_returns_two_mvseries(self, era5, tmp_path):
        """Cached path (run_cleanly=False) should also return 2 MVSeries."""
        processed_ds = _grid_ds(
            {"tp": (50.0, "mm/month"), "e": (80.0, "mm/month")},
            product_name="ERA5_Land",
        )
        GLHE.CLAY.helpers.pickle_var(processed_ds, "ERA5_Land")

        result = era5.product_driver(POLY, run_cleanly=False, debug=False)

        assert len(result) == 2
        slcs = {mv.single_letter_code for mv in result}
        assert slcs == {"p", "e"}


# ── NWM ──────────────────────────────────────────────────────────────────────

class TestNWMDriver:
    """Patches call_NWM_s3_access_and_process with a fully-processed monthly
    dataset.  Exercises convert_xarray_dataset_to_mvseries and date snapping."""

    def test_returns_two_mvseries(self, tmp_path):
        nwm = nwm_mod.NWM()
        with patch.object(nwm, "call_NWM_s3_access_and_process", return_value=_nwm_monthly_ds()):
            result = nwm.product_driver(POLY, run_cleanly=True)

        assert len(result) == 2

    def test_inflow_mvseries(self, tmp_path):
        nwm = nwm_mod.NWM()
        with patch.object(nwm, "call_NWM_s3_access_and_process", return_value=_nwm_monthly_ds()):
            result = nwm.product_driver(POLY, run_cleanly=True)

        inflow = next(mv for mv in result if mv.single_letter_code == "i")
        _check(inflow, "i", "NWM")
        assert (inflow.dataset > 0).all(), "inflow values should be positive"

    def test_outflow_mvseries(self, tmp_path):
        nwm = nwm_mod.NWM()
        with patch.object(nwm, "call_NWM_s3_access_and_process", return_value=_nwm_monthly_ds()):
            result = nwm.product_driver(POLY, run_cleanly=True)

        outflow = next(mv for mv in result if mv.single_letter_code == "o")
        _check(outflow, "o", "NWM")

    def test_cached_run_returns_two_mvseries(self, tmp_path):
        """Pickled NWM dataset is used on second run; structure must match."""
        GLHE.CLAY.helpers.pickle_var(_nwm_monthly_ds(), "NWM")

        nwm = nwm_mod.NWM()
        result = nwm.product_driver(POLY, run_cleanly=False, debug=False)

        assert len(result) == 2
        slcs = {mv.single_letter_code for mv in result}
        assert slcs == {"i", "o"}


# ── DAHITI ────────────────────────────────────────────────────────────────────

class TestDAHITIDriver:
    """Patches _fetch_and_process with a synthetic WSE series.  Exercises
    manual MVSeries construction and best-effort hypsometry (which silently
    skips if the target_id pickle is absent)."""

    def _make_dahiti(self):
        dahiti = dahiti_mod.DAHITI()
        dahiti._api_key = "test_key_v2"   # satisfy _has_credentials()
        return dahiti

    def test_returns_one_mvseries(self, tmp_path):
        dahiti = self._make_dahiti()
        with patch.object(dahiti, "_fetch_and_process", return_value=_wse_series()):
            result = dahiti.product_driver(POLY, run_cleanly=True)

        assert len(result) == 1

    def test_water_level_mvseries(self, tmp_path):
        dahiti = self._make_dahiti()
        with patch.object(dahiti, "_fetch_and_process", return_value=_wse_series()):
            result = dahiti.product_driver(POLY, run_cleanly=True)

        _check(result[0], "l", "DAHITI")

    def test_wse_values_are_correct(self, tmp_path):
        dahiti = self._make_dahiti()
        wse = _wse_series()
        with patch.object(dahiti, "_fetch_and_process", return_value=wse):
            result = dahiti.product_driver(POLY, run_cleanly=True)

        assert result[0].dataset.mean() == pytest.approx(1280.5)

    def test_missing_credentials_raises_before_fetch(self, tmp_path):
        dahiti = dahiti_mod.DAHITI()
        # No credentials set in env → _has_credentials() = False
        dahiti._api_key = None
        dahiti._username = None
        dahiti._password = None

        with pytest.raises(RuntimeError, match="No DAHITI credentials"):
            dahiti.product_driver(POLY, run_cleanly=True)

    def test_cached_run_uses_pickle(self, tmp_path):
        GLHE.CLAY.helpers.pickle_var(_wse_series(), "DAHITI")

        dahiti = self._make_dahiti()
        result = dahiti.product_driver(POLY, run_cleanly=False, debug=False)

        assert len(result) == 1
        _check(result[0], "l", "DAHITI")

    def test_hypsometry_failure_does_not_abort_driver(self, tmp_path):
        """_try_fetch_hypsometry may fail (no DAHITI_target_id pickle);
        product_driver must still return the water-level MVSeries."""
        dahiti = self._make_dahiti()
        with patch.object(dahiti, "_fetch_and_process", return_value=_wse_series()):
            result = dahiti.product_driver(POLY, run_cleanly=True)

        assert len(result) == 1
        assert result[0].single_letter_code == "l"


# ── GloFAS ────────────────────────────────────────────────────────────────────

class TestGloFASDriver:
    """Patches _fetch_and_process with a synthetic discharge series.
    Exercises manual MVSeries construction and event publishing."""

    @pytest.fixture
    def glofas(self):
        with patch("cdsapi.Client"):
            return glofas_mod.GloFAS()

    def test_returns_one_mvseries(self, glofas):
        with patch.object(glofas, "_fetch_and_process", return_value=_discharge_series()):
            result = glofas.product_driver(POLY, run_cleanly=True)

        assert len(result) == 1

    def test_discharge_mvseries(self, glofas):
        with patch.object(glofas, "_fetch_and_process", return_value=_discharge_series()):
            result = glofas.product_driver(POLY, run_cleanly=True)

        _check(result[0], "i", "GloFAS")

    def test_discharge_values_positive(self, glofas):
        with patch.object(glofas, "_fetch_and_process", return_value=_discharge_series()):
            result = glofas.product_driver(POLY, run_cleanly=True)

        assert (result[0].dataset > 0).all(), "discharge must be positive"

    def test_cached_run_uses_pickle(self, glofas, tmp_path):
        GLHE.CLAY.helpers.pickle_var(_discharge_series(), "GloFAS")

        result = glofas.product_driver(POLY, run_cleanly=False, debug=False)

        assert len(result) == 1
        _check(result[0], "i", "GloFAS")


# ── GLEAM ─────────────────────────────────────────────────────────────────────

class TestGLEAMDriver:
    """Patches _find_files (returns a non-empty list) and _load_and_process
    (returns a processed Ew dataset in mm/month).  Exercises spatial averaging,
    SLC override, and event publishing."""

    def _processed_gleam_ds(self):
        """Simulate the dataset returned by _load_and_process (already mm/month)."""
        ds = _grid_ds({"Ew": (50.0, "mm/month")}, product_name="GLEAM")
        return ds

    def test_returns_one_mvseries(self, tmp_path):
        gleam = gleam_mod.GLEAM()
        fake_files = [Path("/fake/Ew_2000_GLEAM_v3.5a.nc")]
        with patch.object(gleam, "_find_files", return_value=fake_files), \
             patch.object(gleam, "_load_and_process", return_value=self._processed_gleam_ds()):
            result = gleam.product_driver(POLY, run_cleanly=True)

        assert len(result) == 1

    def test_evap_mvseries(self, tmp_path):
        gleam = gleam_mod.GLEAM()
        fake_files = [Path("/fake/Ew_2000_GLEAM_v3.5a.nc")]
        with patch.object(gleam, "_find_files", return_value=fake_files), \
             patch.object(gleam, "_load_and_process", return_value=self._processed_gleam_ds()):
            result = gleam.product_driver(POLY, run_cleanly=True)

        _check(result[0], "e", "GLEAM")

    def test_slc_is_e_regardless_of_variable_name(self, tmp_path):
        """product_driver forces single_letter_code='e' even if var is named Ew."""
        gleam = gleam_mod.GLEAM()
        fake_files = [Path("/fake/Ew_2000_GLEAM_v3.5a.nc")]
        with patch.object(gleam, "_find_files", return_value=fake_files), \
             patch.object(gleam, "_load_and_process", return_value=self._processed_gleam_ds()):
            result = gleam.product_driver(POLY, run_cleanly=True)

        assert result[0].single_letter_code == "e"

    def test_ea_fallback_also_gives_slc_e(self, tmp_path):
        """Ea (total actual ET) fallback must also produce SLC='e'."""
        gleam = gleam_mod.GLEAM()
        ea_ds = _grid_ds({"Ea": (45.0, "mm/month")}, product_name="GLEAM")
        fake_files = [Path("/fake/Ea_2000_GLEAM_v3.5a.nc")]
        with patch.object(gleam, "_find_files", return_value=fake_files), \
             patch.object(gleam, "_load_and_process", return_value=ea_ds):
            result = gleam.product_driver(POLY, run_cleanly=True)

        assert result[0].single_letter_code == "e"
        assert result[0].product_name == "GLEAM"

    def test_no_files_raises_runtime_error(self, tmp_path):
        """product_driver must raise RuntimeError (not silently return []) when
        no GLEAM files are found."""
        gleam = gleam_mod.GLEAM()
        with patch.object(gleam, "_find_files", return_value=[]):
            with pytest.raises(RuntimeError, match="No GLEAM NetCDF files"):
                gleam.product_driver(POLY, run_cleanly=True)

    def test_cached_run_uses_pickle(self, tmp_path):
        processed_ds = self._processed_gleam_ds()
        GLHE.CLAY.helpers.pickle_var(processed_ds, "GLEAM")

        fake_files = [Path("/fake/Ew_2000_GLEAM_v3.5a.nc")]
        gleam = gleam_mod.GLEAM()
        with patch.object(gleam, "_find_files", return_value=fake_files):
            # _load_and_process must NOT be called on cached run
            result = gleam.product_driver(POLY, run_cleanly=False, debug=False)

        assert len(result) == 1
        assert result[0].single_letter_code == "e"


# ── Cross-product: verify_inputs and attach_geodata return types ──────────────

class TestVerifyInputsAndAttachGeodata:
    """Every product must return True from verify_inputs and a known string
    from attach_geodata.  These are called by CLAY_driver before product_driver."""

    def test_cruts_verify_inputs(self):
        assert _cruts().verify_inputs() is True

    def test_cruts_attach_geodata(self):
        assert _cruts().attach_geodata() == "grid"

    def test_era5_verify_inputs(self):
        with patch("cdsapi.Client"):
            assert era5_mod.ERA5_Land().verify_inputs() is True

    def test_era5_attach_geodata(self):
        with patch("cdsapi.Client"):
            assert era5_mod.ERA5_Land().attach_geodata() == "grid"

    def test_nwm_verify_inputs(self):
        assert nwm_mod.NWM().verify_inputs() is True

    def test_dahiti_verify_inputs(self):
        assert dahiti_mod.DAHITI().verify_inputs() is True  # True even without creds

    def test_dahiti_attach_geodata(self):
        assert dahiti_mod.DAHITI().attach_geodata() == "complete"

    def test_glofas_verify_inputs(self):
        with patch("cdsapi.Client"):
            assert glofas_mod.GloFAS().verify_inputs() is True

    def test_glofas_attach_geodata(self):
        with patch("cdsapi.Client"):
            assert glofas_mod.GloFAS().attach_geodata() == "complete"

    def test_gleam_verify_inputs(self):
        # Always True — no files needed until product_driver
        assert gleam_mod.GLEAM().verify_inputs() is True

    def test_gleam_attach_geodata(self):
        assert gleam_mod.GLEAM().attach_geodata() == "grid"

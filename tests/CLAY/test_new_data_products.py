"""Unit tests for DAHITI, GLEAM, and GloFAS data access classes.

These tests are fast (no network, no real files) and rely on mocking.
Tests that require live API calls are marked @pytest.mark.network and are
skipped by default: pytest -m "not network".
"""

import pytest
import pandas as pd
import numpy as np
import xarray as xr
from calendar import monthrange
from pathlib import Path
from unittest.mock import MagicMock, patch

import GLHE.CLAY.globals
import GLHE.CLAY.helpers


# ---------------------------------------------------------------------------
# DAHITI
# ---------------------------------------------------------------------------


class TestDAHITICredentials:

    def test_verify_inputs_true_even_without_credentials(self, monkeypatch):
        monkeypatch.delenv("DAHITI_API_KEY", raising=False)
        monkeypatch.delenv("DAHITI_USERNAME", raising=False)
        monkeypatch.delenv("DAHITI_PASSWORD", raising=False)
        from GLHE.CLAY.data_access import DAHITI
        GLHE.CLAY.helpers.setup_logging_directory(".temp_tests")
        d = DAHITI.DAHITI()
        assert d.verify_inputs() is True

    def test_no_credentials_returns_false(self, monkeypatch):
        monkeypatch.delenv("DAHITI_API_KEY", raising=False)
        monkeypatch.delenv("DAHITI_USERNAME", raising=False)
        monkeypatch.delenv("DAHITI_PASSWORD", raising=False)
        from GLHE.CLAY.data_access import DAHITI
        d = DAHITI.DAHITI()
        assert d._has_credentials() is False

    def test_api_key_sets_v2(self, monkeypatch):
        monkeypatch.setenv("DAHITI_API_KEY", "mykey")
        from GLHE.CLAY.data_access import DAHITI
        d = DAHITI.DAHITI()
        assert d._has_credentials() is True
        assert d._uses_v2() is True

    def test_username_password_sets_v1(self, monkeypatch):
        monkeypatch.delenv("DAHITI_API_KEY", raising=False)
        monkeypatch.setenv("DAHITI_USERNAME", "user")
        monkeypatch.setenv("DAHITI_PASSWORD", "pass")
        from GLHE.CLAY.data_access import DAHITI
        d = DAHITI.DAHITI()
        assert d._has_credentials() is True
        assert d._uses_v2() is False


class TestDAHITIWaterLevel:

    def _make_dahiti(self, monkeypatch):
        monkeypatch.setenv("DAHITI_API_KEY", "testkey")
        from GLHE.CLAY.data_access import DAHITI
        GLHE.CLAY.helpers.setup_logging_directory(".temp_tests")
        return DAHITI.DAHITI()

    def test_monthly_average_two_obs_same_month(self, monkeypatch):
        """Two observations in January are averaged into one monthly value."""
        d = self._make_dahiti(monkeypatch)
        sample = [
            {"date": "2000-01-15", "wse": 1280.0},
            {"date": "2000-01-25", "wse": 1282.0},
            {"date": "2000-02-10", "wse": 1279.0},
        ]
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp):
            series = d.download_water_level(12345)
        assert len(series) == 2
        assert series.iloc[0] == pytest.approx(1281.0)
        assert series.index[0] == pd.Timestamp("2000-01-01")

    def test_index_snapped_to_month_start(self, monkeypatch):
        d = self._make_dahiti(monkeypatch)
        sample = [{"date": "2001-06-17", "wse": 1000.0}]
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp):
            series = d.download_water_level(99)
        assert series.index[0] == pd.Timestamp("2001-06-01")


class TestDAHITIHypsometry:

    def _make_dahiti(self, monkeypatch):
        monkeypatch.setenv("DAHITI_API_KEY", "testkey")
        from GLHE.CLAY.data_access import DAHITI
        GLHE.CLAY.helpers.setup_logging_directory(".temp_tests")
        return DAHITI.DAHITI()

    def test_sorted_ascending_by_wse(self, monkeypatch):
        d = self._make_dahiti(monkeypatch)
        sample = [
            {"wse": 1282.0, "area": 3500.0},
            {"wse": 1278.0, "area": 2500.0},
            {"wse": 1280.0, "area": 3000.0},
        ]
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp):
            df = d.download_hypsometry(12345)
        assert list(df.columns) == ["water_surface_elevation_m", "surface_area_km2"]
        assert df["water_surface_elevation_m"].iloc[0] == pytest.approx(1278.0)
        assert df["water_surface_elevation_m"].iloc[-1] == pytest.approx(1282.0)
        assert len(df) == 3

    def test_empty_response_raises(self, monkeypatch):
        d = self._make_dahiti(monkeypatch)
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp):
            with pytest.raises(RuntimeError, match="No hypsometry data"):
                d.download_hypsometry(12345)

    def test_area_field_variants_parsed(self, monkeypatch):
        """DAHITI may use 'area', 'surface_area', or 'area_km2' field name."""
        d = self._make_dahiti(monkeypatch)
        sample = [{"wse": 1280.0, "surface_area": 3000.0}]
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample
        mock_resp.raise_for_status.return_value = None
        with patch("requests.get", return_value=mock_resp):
            df = d.download_hypsometry(1)
        assert df["surface_area_km2"].iloc[0] == pytest.approx(3000.0)

    def test_uses_v1_post_when_no_api_key(self, monkeypatch):
        monkeypatch.delenv("DAHITI_API_KEY", raising=False)
        monkeypatch.setenv("DAHITI_USERNAME", "user")
        monkeypatch.setenv("DAHITI_PASSWORD", "pass")
        from GLHE.CLAY.data_access import DAHITI
        GLHE.CLAY.helpers.setup_logging_directory(".temp_tests")
        d = DAHITI.DAHITI()
        sample = [{"wse": 1280.0, "area": 3000.0}]
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample
        mock_resp.raise_for_status.return_value = None
        with patch("requests.post", return_value=mock_resp) as mock_post:
            df = d.download_hypsometry(1)
        mock_post.assert_called_once()
        assert df["water_surface_elevation_m"].iloc[0] == pytest.approx(1280.0)


# ---------------------------------------------------------------------------
# GLEAM
# ---------------------------------------------------------------------------


class TestGLEAMFileDiscovery:

    def test_verify_inputs_always_true(self):
        from GLHE.CLAY.data_access import GLEAM
        GLHE.CLAY.helpers.setup_logging_directory(".temp_tests")
        g = GLEAM.GLEAM()
        assert g.verify_inputs() is True

    def test_find_files_empty_when_dir_missing(self, tmp_path, monkeypatch):
        import GLHE.CLAY.data_access.GLEAM as gleam_mod
        monkeypatch.setattr(gleam_mod, "_GLEAM_DIR", tmp_path / "nonexistent")
        g = gleam_mod.GLEAM()
        assert g._find_files() == []

    def test_find_files_prefers_ew_over_ea(self, tmp_path, monkeypatch):
        import GLHE.CLAY.data_access.GLEAM as gleam_mod
        monkeypatch.setattr(gleam_mod, "_GLEAM_DIR", tmp_path)
        (tmp_path / "Ew_2000_GLEAM_v3.5a.nc").touch()
        (tmp_path / "Ea_2000_GLEAM_v3.5a.nc").touch()
        g = gleam_mod.GLEAM()
        files = g._find_files()
        assert len(files) == 1
        assert "Ew_" in files[0].name

    def test_find_files_falls_back_to_ea(self, tmp_path, monkeypatch):
        import GLHE.CLAY.data_access.GLEAM as gleam_mod
        monkeypatch.setattr(gleam_mod, "_GLEAM_DIR", tmp_path)
        (tmp_path / "Ea_2000_GLEAM_v3.5a.nc").touch()
        g = gleam_mod.GLEAM()
        files = g._find_files()
        assert len(files) == 1
        assert "Ea_" in files[0].name

    def test_find_files_returns_multiple_years(self, tmp_path, monkeypatch):
        import GLHE.CLAY.data_access.GLEAM as gleam_mod
        monkeypatch.setattr(gleam_mod, "_GLEAM_DIR", tmp_path)
        for yr in (2000, 2001, 2002):
            (tmp_path / f"Ew_{yr}_GLEAM_v3.5a.nc").touch()
        g = gleam_mod.GLEAM()
        files = g._find_files()
        assert len(files) == 3

    def test_product_driver_raises_when_no_files(self, tmp_path, monkeypatch):
        import GLHE.CLAY.data_access.GLEAM as gleam_mod
        monkeypatch.setattr(gleam_mod, "_GLEAM_DIR", tmp_path / "nonexistent")
        GLHE.CLAY.helpers.setup_output_directory("TestLake")
        GLHE.CLAY.helpers.setup_logging_directory(".temp_tests")
        import shapely.geometry
        polygon = shapely.geometry.box(-110.0, 40.0, -109.0, 41.0)
        g = gleam_mod.GLEAM()
        with pytest.raises(RuntimeError, match="No GLEAM NetCDF files"):
            g.product_driver(polygon)


class TestGLEAMUnitConversion:

    def test_mm_day_to_mm_month_january(self):
        """1 mm/day for all of January 2000 (31 days) → 31 mm/month."""
        time = pd.date_range("2000-01-01", periods=3, freq="MS")
        data = np.ones((3, 2, 2), dtype=float)
        ds = xr.Dataset(
            {"Ew": (["time", "lat", "lon"], data)},
            coords={"time": time, "lat": [0.0, 0.25], "lon": [0.0, 0.25]},
        )
        days_arr = np.array(
            [monthrange(int(t.year), int(t.month))[1] for t in time], dtype=float
        )
        result = ds["Ew"] * xr.DataArray(days_arr, dims=["time"])
        assert float(result.isel(time=0, lat=0, lon=0)) == pytest.approx(31.0)

    def test_mm_day_to_mm_month_february_leap(self):
        """Feb 2000 is a leap year — 29 days."""
        time = pd.date_range("2000-02-01", periods=1, freq="MS")
        data = np.ones((1, 1, 1), dtype=float)
        ds = xr.Dataset(
            {"Ew": (["time", "lat", "lon"], data)},
            coords={"time": time, "lat": [0.0], "lon": [0.0]},
        )
        days_arr = np.array(
            [monthrange(int(t.year), int(t.month))[1] for t in time], dtype=float
        )
        result = ds["Ew"] * xr.DataArray(days_arr, dims=["time"])
        assert float(result.isel(time=0, lat=0, lon=0)) == pytest.approx(29.0)

    def test_mm_day_to_mm_month_february_non_leap(self):
        """Feb 2001 is not a leap year — 28 days."""
        time = pd.date_range("2001-02-01", periods=1, freq="MS")
        data = np.ones((1, 1, 1), dtype=float)
        ds = xr.Dataset(
            {"Ew": (["time", "lat", "lon"], data)},
            coords={"time": time, "lat": [0.0], "lon": [0.0]},
        )
        days_arr = np.array(
            [monthrange(int(t.year), int(t.month))[1] for t in time], dtype=float
        )
        result = ds["Ew"] * xr.DataArray(days_arr, dims=["time"])
        assert float(result.isel(time=0, lat=0, lon=0)) == pytest.approx(28.0)


# ---------------------------------------------------------------------------
# GloFAS
# ---------------------------------------------------------------------------


class TestGloFAS:

    @pytest.fixture
    def glofas(self):
        with patch("cdsapi.Client"):
            from GLHE.CLAY.data_access import GloFAS
            GLHE.CLAY.helpers.setup_logging_directory(".temp_tests")
            return GloFAS.GloFAS()

    def test_verify_inputs_always_true(self, glofas):
        assert glofas.verify_inputs() is True

    def test_attach_geodata_returns_complete(self, glofas):
        assert glofas.attach_geodata() == "complete"

    def test_open_grib_raises_on_invalid_file(self, glofas, tmp_path):
        dummy = tmp_path / "bad.grib"
        dummy.write_bytes(b"not a real grib file content")
        with pytest.raises(RuntimeError):
            glofas._open_grib(str(dummy))

    def test_discharge_m3s_to_m3month_conversion(self):
        """m³/s × seconds_in_month = m³/month."""
        from calendar import monthrange
        import pandas as pd
        time = pd.date_range("2000-01-01", periods=2, freq="MS")
        series = pd.Series([1.0, 1.0], index=time, name="dis24")
        days = series.index.map(lambda t: monthrange(t.year, t.month)[1])
        converted = series * days * 86400
        assert converted.iloc[0] == pytest.approx(31 * 86400)
        assert converted.iloc[1] == pytest.approx(29 * 86400)  # Feb 2000 leap

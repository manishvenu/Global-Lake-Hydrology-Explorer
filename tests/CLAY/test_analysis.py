"""Unit tests for CLAY analysis functions: trend statistics and water balance.

All tests are fast and fully offline — they patch GLHE.CLAY.globals.config
to write output to pytest's tmp_path rather than the real output directory.
"""

import pytest
import pandas as pd
import numpy as np

import GLHE.CLAY.globals
import GLHE.CLAY.helpers
from GLHE.CLAY import combined_data_functions


@pytest.fixture(autouse=True)
def isolate_output(tmp_path):
    """Redirect all file output to tmp_path for test isolation."""
    orig_out = GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
    orig_name = GLHE.CLAY.globals.config.get("LAKE_NAME", "")
    GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] = str(tmp_path)
    GLHE.CLAY.globals.config["LAKE_NAME"] = "TestLake"
    GLHE.CLAY.helpers.setup_logging_directory(".temp_tests")
    yield tmp_path
    GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] = orig_out
    GLHE.CLAY.globals.config["LAKE_NAME"] = orig_name


# ---------------------------------------------------------------------------
# compute_trend_statistics
# ---------------------------------------------------------------------------


class TestComputeTrendStatistics:

    def _make_df(self, values, col="p.ERA5"):
        idx = pd.date_range("2000-01-01", periods=len(values), freq="MS")
        return pd.DataFrame({col: values}, index=idx)

    def test_increasing_trend_detected(self, tmp_path):
        rng = np.random.RandomState(42)
        vals = np.arange(60, dtype=float) + rng.normal(0, 0.05, 60)
        combined_data_functions.compute_trend_statistics(self._make_df(vals))
        out = pd.read_csv(tmp_path / "TestLake_trend_stats.csv")
        assert out.iloc[0]["trend_direction"] == "increasing"
        assert out.iloc[0]["significant_at_0.05"] == True

    def test_decreasing_trend_detected(self, tmp_path):
        vals = np.arange(60, 0, -1, dtype=float)
        combined_data_functions.compute_trend_statistics(self._make_df(vals))
        out = pd.read_csv(tmp_path / "TestLake_trend_stats.csv")
        assert out.iloc[0]["trend_direction"] == "decreasing"
        assert out.iloc[0]["significant_at_0.05"] == True

    def test_short_series_skipped_no_file(self, tmp_path):
        combined_data_functions.compute_trend_statistics(self._make_df([1.0] * 5))
        assert not (tmp_path / "TestLake_trend_stats.csv").exists()

    def test_exactly_ten_obs_not_skipped(self, tmp_path):
        combined_data_functions.compute_trend_statistics(
            self._make_df(list(range(10)))
        )
        assert (tmp_path / "TestLake_trend_stats.csv").exists()

    def test_output_has_expected_columns(self, tmp_path):
        combined_data_functions.compute_trend_statistics(self._make_df(range(24)))
        out = pd.read_csv(tmp_path / "TestLake_trend_stats.csv")
        expected = {
            "variable",
            "n_months",
            "mann_kendall_tau",
            "p_value",
            "significant_at_0.05",
            "trend_direction",
            "sen_slope_per_month",
            "sen_slope_per_year",
        }
        assert expected.issubset(set(out.columns))

    def test_sen_slope_per_year_is_twelve_times_monthly(self, tmp_path):
        combined_data_functions.compute_trend_statistics(self._make_df(range(36)))
        out = pd.read_csv(tmp_path / "TestLake_trend_stats.csv")
        row = out.iloc[0]
        assert row["sen_slope_per_year"] == pytest.approx(
            row["sen_slope_per_month"] * 12, rel=1e-3
        )

    def test_multiple_columns_each_get_a_row(self, tmp_path):
        idx = pd.date_range("2000-01-01", periods=24, freq="MS")
        df = pd.DataFrame(
            {"p.ERA5": np.arange(24), "e.ERA5": np.arange(24, 48)}, index=idx
        )
        combined_data_functions.compute_trend_statistics(df)
        out = pd.read_csv(tmp_path / "TestLake_trend_stats.csv")
        assert len(out) == 2

    def test_n_months_matches_non_null_count(self, tmp_path):
        vals = [float(i) for i in range(20)]
        vals[5] = float("nan")
        vals[10] = float("nan")
        combined_data_functions.compute_trend_statistics(self._make_df(vals))
        out = pd.read_csv(tmp_path / "TestLake_trend_stats.csv")
        assert out.iloc[0]["n_months"] == 18


# ---------------------------------------------------------------------------
# compute_water_balance
# ---------------------------------------------------------------------------


class TestComputeWaterBalance:

    def _make_idx(self, n=12):
        return pd.date_range("2000-01-01", periods=n, freq="MS")

    def test_p_minus_e_correct(self, tmp_path):
        df = pd.DataFrame(
            {"p.ERA5": [100.0] * 12, "e.ERA5": [60.0] * 12},
            index=self._make_idx(),
        )
        combined_data_functions.compute_water_balance(df)
        wb = pd.read_csv(tmp_path / "TestLake_water_balance.csv", index_col=0, parse_dates=True)
        assert "P_minus_E_mm_month" in wb.columns
        assert wb["P_minus_E_mm_month"].mean() == pytest.approx(40.0)

    def test_p_and_e_means_correct(self, tmp_path):
        df = pd.DataFrame(
            {
                "p.ERA5": [80.0] * 12,
                "p.CRUTS": [100.0] * 12,
                "e.ERA5": [50.0] * 12,
                "e.GLEAM": [70.0] * 12,
            },
            index=self._make_idx(),
        )
        combined_data_functions.compute_water_balance(df)
        wb = pd.read_csv(tmp_path / "TestLake_water_balance.csv", index_col=0, parse_dates=True)
        assert wb["P_mean_mm_month"].mean() == pytest.approx(90.0)
        assert wb["E_mean_mm_month"].mean() == pytest.approx(60.0)

    def test_inflow_included_when_present(self, tmp_path):
        df = pd.DataFrame(
            {
                "p.ERA5": [100.0] * 12,
                "e.ERA5": [60.0] * 12,
                "i.GloFAS": [1.0e8] * 12,
            },
            index=self._make_idx(),
        )
        combined_data_functions.compute_water_balance(df)
        wb = pd.read_csv(tmp_path / "TestLake_water_balance.csv", index_col=0, parse_dates=True)
        assert "Inflow_mean_m3_month" in wb.columns
        assert wb["Inflow_mean_m3_month"].mean() == pytest.approx(1.0e8)

    def test_water_level_included_when_present(self, tmp_path):
        df = pd.DataFrame(
            {
                "p.ERA5": [100.0] * 12,
                "e.ERA5": [60.0] * 12,
                "l.DAHITI": [1280.5] * 12,
            },
            index=self._make_idx(),
        )
        combined_data_functions.compute_water_balance(df)
        wb = pd.read_csv(tmp_path / "TestLake_water_balance.csv", index_col=0, parse_dates=True)
        assert "Water_Level_mean_m" in wb.columns
        assert wb["Water_Level_mean_m"].mean() == pytest.approx(1280.5)

    def test_no_p_or_e_skips_file(self, tmp_path):
        df = pd.DataFrame(
            {"i.GloFAS": [1.0e8] * 12},
            index=self._make_idx(),
        )
        combined_data_functions.compute_water_balance(df)
        assert not (tmp_path / "TestLake_water_balance.csv").exists()

    def test_only_p_no_e_no_p_minus_e(self, tmp_path):
        df = pd.DataFrame({"p.ERA5": [100.0] * 12}, index=self._make_idx())
        combined_data_functions.compute_water_balance(df)
        wb = pd.read_csv(tmp_path / "TestLake_water_balance.csv", index_col=0, parse_dates=True)
        assert "P_mean_mm_month" in wb.columns
        assert "P_minus_E_mm_month" not in wb.columns

    def test_empty_dataframe_skips_file(self, tmp_path):
        combined_data_functions.compute_water_balance(pd.DataFrame())
        assert not (tmp_path / "TestLake_water_balance.csv").exists()

    def test_ensemble_mean_across_products(self, tmp_path):
        """Three inflow products → ensemble mean."""
        df = pd.DataFrame(
            {
                "p.ERA5": [100.0] * 12,
                "e.ERA5": [60.0] * 12,
                "i.GloFAS": [1.0e8] * 12,
                "i.NWM": [2.0e8] * 12,
            },
            index=self._make_idx(),
        )
        combined_data_functions.compute_water_balance(df)
        wb = pd.read_csv(tmp_path / "TestLake_water_balance.csv", index_col=0, parse_dates=True)
        assert wb["Inflow_mean_m3_month"].mean() == pytest.approx(1.5e8)

"""Tests for the LIME view components (overview, series, gridded, table)."""

from pathlib import Path

import numpy as np
import pandas as pd
import panel as pn
import param
import pytest

from GLHE.LIME import theme
from GLHE.LIME.components import gridded, overview, series, table
from GLHE.LIME.state import AppState

FIXTURE = Path(__file__).parent / "fixtures" / "Fixture_Lake"
ZIP = FIXTURE / "Fixture_Lake_gridded_data_layers_on_20000101.zip"


@pytest.fixture(scope="module", autouse=True)
def _theme():
    theme.apply_theme(dark=True)


@pytest.fixture
def state():
    s = AppState()
    s.load_output_dir(FIXTURE)
    return s


class FakeGlobe(param.Parameterized):
    """Stands in for LakeGlobe: only the param the gridded view writes."""

    cells_geojson = param.Dict(default={})


def _render(viewable):
    """Build the bokeh model, which is where most Panel/HoloViews errors surface."""
    return viewable.get_root(pn.io.document.Document())


# -- pure functions ---------------------------------------------------------


def test_mean_annual_uses_complete_years(state):
    s = state.df["p.CRUTS"]  # NaN from 2023 on
    totals = overview.annual_totals(s)
    assert totals.index.max().year == 2022
    assert overview.mean_annual(s) == pytest.approx(totals.mean())


def test_lake_area_positive(state):
    area = overview.lake_area_km2(state.polygon_geojson)
    assert area is not None and area > 0
    assert overview.lake_area_km2({}) is None


def test_p_minus_e_and_climatology(state):
    pe = overview.p_minus_e(state.df)
    assert set(pe) == {"ERA5-Land", "CRUTS"}
    clim = overview.climatology(state.df)
    assert list(clim.index) == list(range(1, 13))


def test_series_annual_aggregate(state):
    annual = series.aggregate(state.df, series.ANNUAL)
    assert annual.index.freqstr.startswith("YS")
    assert annual["i.NWM"].first_valid_index().year == 2002


def test_gridded_layers_and_cells():
    layers = gridded.list_layers(ZIP)
    keys = {layer["key"] for layer in layers}
    assert keys == {"p.ERA5-Land", "e.ERA5-Land", "p.CRUTS", "e.CRUTS"}
    assert all(layer["date"] == pd.Timestamp("2000-01-01") for layer in layers)
    member = next(lyr["member"] for lyr in layers if lyr["key"] == "p.ERA5-Land")
    data, transform, bounds = gridded.read_layer(ZIP, member)
    assert data.shape == (10, 10)
    assert bounds == pytest.approx((-112.9, 40.7, -111.9, 41.7))
    fc = gridded.cells_geojson(data, transform, {"slc": "p"})
    assert len(fc["features"]) == np.isfinite(data).sum()
    props = fc["features"][0]["properties"]
    assert props["color"].startswith("#") and props["slc"] == "p"


def test_parse_layer_name_with_underscored_lake():
    info = gridded.parse_layer_name("e_CRUTS_Great_Salt_Lake_20240301.tif")
    assert info["key"] == "e.CRUTS"
    assert info["date"] == pd.Timestamp("2024-03-01")
    assert gridded.parse_layer_name("readme.txt") is None


# -- views build with the fixture and with an empty state -------------------


@pytest.mark.parametrize(
    "factory",
    [
        overview.make_overview,
        series.make_series,
        gridded.make_gridded,
        table.make_table,
    ],
)
def test_views_build(state, factory):
    _render(factory(state))


@pytest.mark.parametrize(
    "factory",
    [
        overview.make_overview,
        series.make_series,
        gridded.make_gridded,
        table.make_table,
    ],
)
def test_views_empty_state(factory):
    _render(factory(AppState()))


def test_gridded_pushes_cells_to_globe(state):
    globe = FakeGlobe()
    _render(gridded.make_gridded(state, globe=globe))
    assert globe.cells_geojson["type"] == "FeatureCollection"
    assert len(globe.cells_geojson["features"]) > 0
    # Switching to a lake without data clears the globe.
    state.param.update(output_dir="", config={}, df=None)
    assert globe.cells_geojson == {}


def test_views_react_to_state_change():
    s = AppState()
    view = series.make_series(s)
    _render(view)
    s.load_output_dir(FIXTURE)  # should rebuild without error
    _render(view)


def test_table_downloads(state):
    frame = table.table_frame(state.df)
    assert frame.columns[0] == "Date" and frame["Date"].iloc[0] == "2000-01"
    view = table.make_table(state)
    _render(view)


@pytest.mark.parametrize("mode", [series.MONTHLY, series.ANNUAL])
def test_series_layout_modes(state, mode):
    view = series.series_layout(state.df, ["ERA5-Land", "NWM"], mode, dark=True)
    _render(view)
    # No products selected -> friendly message instead of an error.
    _render(series.series_layout(state.df, [], mode, dark=True))


def test_gridded_bad_tif_shows_message(tmp_path):
    """A corrupt GeoTIFF inside the zip shows a message, not a traceback."""
    import zipfile

    bad = tmp_path / "Bad_gridded_data_layers_on_20000101.zip"
    with zipfile.ZipFile(bad, "w") as zf:
        zf.writestr("p_ERA5-Land_Bad_20000101.tif", b"not a tiff")
    globe = FakeGlobe()
    view = gridded._build(bad, {}, globe)
    _render(view)
    html_objects = [
        p.object for p in view.select(pn.pane.HTML) if isinstance(p.object, str)
    ]
    assert any("Could not read" in obj for obj in html_objects)
    assert globe.cells_geojson == {}


def test_overview_number_format():
    assert overview._fmt(1.39e9, "m³/yr") == "1.39 km³/yr"
    assert overview._fmt(2.5e7, "") == "25 million"
    assert overview._fmt(1234.0, "mm/yr") == "1,234 mm/yr"
    assert overview._fmt(None) == "n/a"


def test_table_titles_are_readable(state):
    tab = table.make_data_table(table.table_frame(state.df))
    assert tab.titles["p.ERA5-Land"] == "Precip. · ERA5-Land (mm/month)"


def test_charts_do_not_zoom_on_wheel(state):
    """The mouse wheel scrolls the page; zoom is opt-in from the toolbar."""
    from bokeh.models import Plot

    lay = series.series_layout(
        state.df, list(series.products_in(state.df)), series.MONTHLY, True
    )
    root = _render(lay)
    plots = list(root.select({"type": Plot}))
    assert plots and all(p.toolbar.active_scroll is None for p in plots)


# ------------------------------------------------------------ water balance
def _lake_with_water_balance(tmp_path, with_hypsometry=True):
    """Copy of the fixture lake plus a water_balance.csv (and hypsometry)."""
    import json
    import shutil

    out = tmp_path / "Fixture_Lake"
    shutil.copytree(FIXTURE, out)
    idx = pd.date_range("2000-01-01", periods=24, freq="MS")
    wb = pd.DataFrame(
        {
            "P_mean_mm_month": np.linspace(20, 60, 24),
            "E_mean_mm_month": np.linspace(10, 90, 24),
            "Inflow_mean_m3_month": np.full(24, 5e8),
            "Water_Level_mean_m": np.linspace(1280, 1281, 24),
        },
        index=idx,
    )
    wb["P_minus_E_mm_month"] = wb["P_mean_mm_month"] - wb["E_mean_mm_month"]
    wb.to_csv(out / "Fixture_Lake_water_balance.csv")
    config = json.loads((out / "config.json").read_text())
    config["WATER_BALANCE_DATA"] = "Fixture_Lake_water_balance.csv"
    if with_hypsometry:
        pd.DataFrame(
            {
                "water_surface_elevation_m": [1279, 1280, 1281],
                "surface_area_km2": [3000, 4000, 4500],
            }
        ).to_csv(out / "hyps.csv", index=False)
        config["HYPSOMETRY_DATA"] = "hyps.csv"
    (out / "config.json").write_text(json.dumps(config))
    s = AppState()
    s.load_output_dir(out)
    return s


def test_water_balance_charts_all_panels(tmp_path):
    from GLHE.LIME.components import water_balance as wbm

    s = _lake_with_water_balance(tmp_path)
    wb = wbm.read_water_balance(s.path_for("WATER_BALANCE_DATA"))
    hyps = wbm.read_hypsometry(s.path_for("HYPSOMETRY_DATA"))
    charts = wbm.water_balance_charts(wb, hyps, dark=True)
    titles = [c.opts.get().kwargs.get("title", "") for c in charts]
    assert len(charts) == 4
    assert any("P − E" in t for t in titles)
    assert any("Hypsometric" in t for t in titles)
    _render(wbm.make_water_balance(s))


def test_water_balance_empty_without_csv(state):
    from GLHE.LIME.components import water_balance as wbm

    assert wbm.read_water_balance(state.path_for("WATER_BALANCE_DATA")) is None
    _render(wbm.make_water_balance(state))
    assert "Re-run CLAY" in wbm.water_balance_view(state).object


def test_water_balance_empty_without_lake():
    from GLHE.LIME.components import water_balance as wbm

    _render(wbm.make_water_balance(AppState()))
    assert "No lake loaded" in wbm.water_balance_view(AppState()).object

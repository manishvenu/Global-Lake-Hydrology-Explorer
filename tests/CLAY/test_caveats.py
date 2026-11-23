"""Tests for GLHE.CLAY.caveats (the caveat rules) and LIME's Caveats tab."""

import json
from pathlib import Path

import pytest

from GLHE.CLAY import caveats as cv
from GLHE.LIME.app import caveats_view, load_lake

FIXTURE = Path(__file__).parents[1] / "LIME" / "fixtures" / "Fixture_Lake"


@pytest.fixture(scope="module")
def lake():
    return load_lake(FIXTURE)


def _short_names(found):
    """'ERA5LAND/ERA5LAND001' -> 'ERA5LAND001'."""
    return [c["name"].rsplit("/", 1)[-1] for c in found]


@pytest.mark.parametrize("rule_file", list(cv.RULE_FILES))
def test_every_rule_file_loads(rule_file):
    registry = cv.load_registry(rule_file)
    assert registry.query() is not None


def test_vendored_file_is_valid_geojson():
    data = json.loads((cv.RULES_DIR / "era5-land.geojson").read_text())
    assert data["queryearth"] == "0.8"


def test_era5land001_found_for_2022_2024(lake):
    found = cv.find_caveats(lake["polygon"], "2022-01-01", "2024-12-31", ["ERA5-Land"])
    assert "ERA5LAND001" in _short_names(found)
    first = next(c for c in found if c["name"].endswith("ERA5LAND001"))
    assert first["level"] == "warning"
    assert first["references"] and first["references"][0]["url"].startswith("http")
    assert first["source_file"] == "era5-land.geojson"


def test_era5land001_not_found_for_2000_2010(lake):
    found = cv.find_caveats(lake["polygon"], "2000-01-01", "2010-12-31", ["ERA5-Land"])
    assert "ERA5LAND001" not in _short_names(found)


def test_shapely_polygon_and_underscore_product_name(lake):
    from shapely.geometry import shape

    poly = shape(lake["polygon"]["features"][0]["geometry"])
    found = cv.find_caveats(poly, "2022-01-01", "2024-12-31", ["ERA5_Land"])
    assert "ERA5LAND001" in _short_names(found)


def test_products_filter_out_other_files(lake):
    found = cv.find_caveats(lake["polygon"], "2022-01-01", "2024-12-31", ["NWM"])
    assert not any(c["name"].startswith("ERA5LAND/") for c in found)


def test_sorted_warnings_first(lake):
    found = cv.caveats_for_dataset(lake["polygon"] or None, lake["df"])
    order = [cv.LEVEL_ORDER[c["level"]] for c in found]
    assert order == sorted(order)


def test_nwm_conus_rules():
    from shapely.geometry import box

    inside = _short_names(
        cv.find_caveats(box(-112.6, 41, -112.2, 41.4), products=["NWM"])
    )
    outside = _short_names(cv.find_caveats(box(30, 0, 31, 1), products=["NWM"]))
    assert "NWM001" in inside and "NWM002" not in inside
    assert "NWM002" in outside and "NWM001" not in outside


def test_small_lake_resolution_rule():
    from shapely.geometry import box

    tiny = box(10.0, 45.0, 10.05, 45.05)  # about 22 km2
    names = _short_names(cv.find_caveats(tiny, products=["ERA5-Land"]))
    assert "ERA5LAND-RES001" in names


def test_vizqueryearth_url():
    assert cv.vizqueryearth_url("era5-land.geojson") == (
        "https://crocodile-cesm.github.io/vizqueryearth/"
        "?src=sites/datasets/era5-land.geojson"
    )
    assert cv.vizqueryearth_url("glhe.geojson") is None


def test_caveats_view(lake):
    assert "issues that apply to this lake: 0" in caveats_view([]).object
    found = cv.caveats_for_dataset(lake["polygon"] or None, lake["df"])
    view = caveats_view(found)
    assert "ERA5LAND001" not in view.object  # titles are shown, not rule names
    assert view.name == f"Caveats ({len(found)})"


def test_nwm_coverage_rule_shows_when_nwm_is_missing():
    """NWM002 explains *missing* NWM columns, so it must match without them."""
    from shapely.geometry import box

    names = _short_names(cv.find_caveats(box(30, 0, 31, 1), products=["ERA5-Land"]))
    assert "NWM002" in names
    assert "NWM001" not in names and "NWM003" not in names


def test_header_only_csv_uses_its_columns():
    """A CSV with columns but no rows must not fall back to every product."""
    import pandas as pd

    df = pd.DataFrame({"p.ERA5-Land": []}, index=pd.DatetimeIndex([], name="Date"))
    names = _short_names(cv.caveats_for_dataset(None, df))
    assert not any(n.startswith("CRUTS") for n in names)


def test_clay_writes_caveats_json(tmp_path, monkeypatch):
    """CLAY's output step writes the matched caveats and lists them in config.json."""
    import pandas as pd
    from shapely.geometry import box

    import GLHE.CLAY.globals as g
    from GLHE.CALCITE import events
    from GLHE.CLAY import combined_data_functions as cdf

    monkeypatch.setitem(g.config, "LAKE_NAME", "Test_Lake")
    monkeypatch.setitem(g.config["DIRECTORIES"], "OUTPUT_DIRECTORY", str(tmp_path))
    seen = {}
    monkeypatch.setattr(
        cdf.pubsub.EventBus,
        "Publish",
        lambda bus, ev: seen.setdefault(ev.LIME_file_type, ev.file_path),
        raising=False,
    )
    dates = pd.date_range("2022-01-31", "2024-12-31", freq="ME", name="Date")
    df = pd.DataFrame({"p.ERA5-Land": 1.0}, index=dates)
    cdf.write_caveats(box(-112.6, 41.0, -112.2, 41.4), df)
    out = tmp_path / "Test_Lake_caveats.json"
    names = _short_names(json.loads(out.read_text()))
    assert "ERA5LAND001" in names
    assert seen[events.TypeOfFileLIME.CAVEATS_DATA] == str(out)

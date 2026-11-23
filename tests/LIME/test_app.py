"""Tests for the LIME page (GLHE/LIME/app.py), without a browser."""

import sys
from pathlib import Path

import panel as pn

from GLHE.LIME import app as lime_app
from GLHE.LIME import gridded, jobs

FIXTURES = Path(__file__).parent / "fixtures"
FIXTURE = FIXTURES / "Fixture_Lake"


def fake_clay(output_dir):
    """A fast stand-in for CLAY that 'writes' ``output_dir``."""
    code = f"print('working'); print('{jobs.OUTPUT_MARKER}{output_dir}')"
    return lambda _id: [sys.executable, "-c", code]


def make_page(**kw):
    kw.setdefault("root", FIXTURES)
    kw.setdefault("job", jobs.ClayJob(command=fake_clay(FIXTURE)))
    template = lime_app.build_app(**kw)
    assert isinstance(template, pn.template.BootstrapTemplate)
    return template.lime


def test_load_lake_reads_the_fixture_folder():
    lake = lime_app.load_lake(FIXTURE)
    assert lake["name"] == "Fixture_Lake" and lake["hylak_id"] == 999999
    assert "p.ERA5-Land" in lake["df"].columns
    assert lake["polygon"]["features"] and lake["points"]["features"]
    assert lake["gridded_zip"].name.endswith(".zip")


def test_page_builds_empty_and_lists_processed_lakes():
    page = make_page()
    assert page.tabs.objects == [] and page.run_button.disabled
    assert str(FIXTURE.resolve()) in page.processed.options.values()


def test_open_a_processed_lake_fills_the_tabs_and_globe():
    page = make_page(output_dir=str(FIXTURE))
    names = [tab.name for tab in page.tabs.objects]
    assert names[0] == "Time series" and names[1].startswith("Caveats")
    assert names[2] == "Data"
    assert page.globe.lake_geojson["features"]
    assert page.selected["hylak_id"] == 999999


def test_gridded_layer_goes_on_the_globe():
    page = make_page(output_dir=str(FIXTURE))
    layers = gridded.list_layers(page.lake["gridded_zip"])
    assert "p_ERA5-Land_Fixture_Lake_20000101.tif" in page.layer.options
    page.layer.value = layers[0]
    cells = page.globe.cells_geojson["features"]
    assert cells and cells[0]["properties"]["color"].startswith("#")
    assert "mm/month" in page.layer_text.object
    page.layer.value = ""
    assert page.globe.cells_geojson == {} and page.layer_text.object == ""


def test_units_on_charts_and_table():
    lake = lime_app.load_lake(FIXTURE)
    assert lime_app.unit_of("i.NWM") == "m³/month"
    assert lime_app.unit_of("p_ERA5-Land_X_20000101.tif") == "mm/month"
    table = lime_app.table_view(lake["df"], "X")[1].value
    assert "p.ERA5-Land (mm/month)" in table.columns
    assert table["Month"].iloc[0] == "2000-01"


def test_search_then_choose_a_result(monkeypatch):
    hits = [
        {"hylak_id": 67, "name": "Great Salt Lake", "country": "US", "area_km2": 4360.0}
    ]
    monkeypatch.setattr(lime_app.hydrolakes, "available", lambda src=None: True)
    monkeypatch.setattr(lime_app.hydrolakes, "search", lambda text, limit=8: hits)
    monkeypatch.setattr(lime_app.hydrolakes, "get_lake", lambda i: None)
    page = make_page()
    page.search.value = "great"
    page.on_search()
    assert page.results.visible
    page.results.value = hits[0]
    assert page.selected["hylak_id"] == 67
    assert "Great Salt Lake" in page.lake_text.object
    assert "4,360 km²" in page.lake_text.object
    assert page.run_button.label == "Run CLAY for #67" and not page.run_button.disabled


def test_search_without_hydrolakes_says_so():
    page = make_page()
    page.search.value = "great"
    page.on_search()
    assert "cannot be read" in page.lake_text.object


def test_clicking_a_lake_on_the_globe_selects_it():
    page = make_page()
    page.globe.picked = {"hylak_id": 12, "name": None, "area_km2": 0.5}
    assert page.selected["hylak_id"] == 12
    assert "Unnamed lake" in page.lake_text.object


def test_run_clay_then_open_the_result(wait):
    page = make_page()
    page.select_lake(999999, {"area_km2": 5.0})
    page.run_clay()
    assert wait(page.job) == "done"
    page.poll_job()
    assert "working" in page.log.value
    assert page.lake is not None and page.lake["name"] == "Fixture_Lake"


def test_tiles_url_from_environment(monkeypatch):
    monkeypatch.setenv("GLHE_LAKE_TILES_URL", "/tiles/lakes.pmtiles")
    assert make_page().globe.lake_tiles_url == "/tiles/lakes.pmtiles"
    monkeypatch.setenv("GLHE_LAKE_TILES_URL", "")
    assert make_page().globe.lake_tiles_url == ""

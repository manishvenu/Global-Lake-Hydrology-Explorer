"""Tests for the assembled LIME app (GLHE/LIME/app.py) using the fixture lake.

Nothing here opens a browser or a server: the page is built and its bokeh
model is rendered into a throwaway document, which is where most Panel errors
show up. The CLAY job is replaced by a tiny fake Python command.
"""

import json
import shutil
import sys
from pathlib import Path

import panel as pn
import pytest

from GLHE.LIME import app as lime_app
from GLHE.LIME import jobs, lake_lookup
from GLHE.LIME.components import sidebar as sb

FIXTURES = Path(__file__).parent / "fixtures"
FIXTURE = FIXTURES / "Fixture_Lake"


def _render(viewable):
    return viewable.get_root(pn.io.document.Document())


def fake_clay_for(output_dir: Path):
    """A fake CLAY command that prints the output marker for ``output_dir``."""

    def command(hylak_id):
        code = f"print('working'); print('GLHE_OUTPUT_DIR={output_dir}')"
        return [sys.executable, "-c", code]

    return command


@pytest.fixture
def idle_job():
    return jobs.ClayJob(command=fake_clay_for(FIXTURE))


def test_build_app_with_fixture(idle_job):
    template = lime_app.build_app(
        output_dir=str(FIXTURE), job=idle_job, roots=[FIXTURES]
    )
    app = template.lime
    assert app.state.lake_name == "Fixture_Lake"
    assert app.state.hylak_id == 999999
    # The globe got the lake, its NWM point and the caveats.
    assert app.globe.lake_geojson["features"]
    assert app.globe.points_geojson["features"]
    assert app.state.caveats, "fixture lake should match some caveats"
    assert app.tabs._names[lime_app.CAVEATS_TAB] == (
        f"Caveats ({len(app.state.caveats)})"
    )
    assert "Fixture Lake" in app.header.object and "999999" in app.header.object
    assert "Fixture Lake" in app.lake_info.object
    # The processed-lake list shows the fixture, highlighted as open.
    labels = [getattr(b, "label", "") for b in app.catalog_list.objects]
    assert any("Fixture Lake" in label for label in labels)
    # Run is enabled because we know the lake's ID.
    assert not app.run_button.disabled
    _render(template.main[0])
    for i in range(len(app.tabs)):
        _render(app.tabs[i])


def test_build_app_empty(idle_job, tmp_path):
    template = lime_app.build_app(job=idle_job, roots=[tmp_path])
    app = template.lime
    assert app.state.df is None and app.state.caveats == []
    assert app.run_button.disabled
    assert "No processed lakes" in app.catalog_list.objects[0].object
    assert "No lake loaded" in app.header.object
    _render(app.tabs)


def test_output_dir_from_env(idle_job, monkeypatch):
    monkeypatch.setenv(lime_app.OUTPUT_DIR_ENV, str(FIXTURE))
    template = lime_app.build_app(job=idle_job, roots=[FIXTURES])
    assert template.lime.state.lake_name == "Fixture_Lake"


def test_bad_output_dir_does_not_crash(idle_job, tmp_path):
    template = lime_app.build_app(
        output_dir=str(tmp_path / "missing"), job=idle_job, roots=[tmp_path]
    )
    assert template.lime.state.output_dir == ""


def test_catalog_click_and_filter(idle_job, tmp_path):
    app = lime_app.build_app(job=idle_job, roots=[FIXTURES]).lime
    button = app.catalog_list.objects[0]
    button.clicks += 1  # same as a click in the browser
    assert app.state.lake_name == "Fixture_Lake"
    app.filter_input.value_input = "nothing like this"
    assert "No match" in app.catalog_list.objects[0].object
    app.filter_input.value_input = "fixture"
    assert len(app.catalog_list.objects) == 1


def test_select_lake_without_hydrolakes(idle_job, monkeypatch, tmp_path):
    monkeypatch.setattr(lake_lookup, "available", lambda source=None: False)
    app = lime_app.build_app(job=idle_job, roots=[tmp_path]).lime
    app.select_lake(67)
    assert app.selected == {"hylak_id": 67}
    assert app.run_button.label == "Run CLAY for #67"
    assert not app.run_button.disabled
    assert "could not be read" in app.lake_info.object


def test_typing_an_id_and_enter_selects_it(idle_job, tmp_path):
    app = lime_app.build_app(job=idle_job, roots=[tmp_path]).lime
    app.search_input.value_input = "42"
    # The index is not loaded (offline tests), but a number is offered as an ID.
    assert app.suggestions.visible and "#42" in app.suggestions[0].label
    app.search_input.param.trigger("enter_pressed")
    assert app.selected["hylak_id"] == 42
    assert app.run_button.label == "Run CLAY for #42"
    assert not app.suggestions.visible


def test_search_while_index_loading_says_so(idle_job, tmp_path):
    app = lime_app.build_app(job=idle_job, roots=[tmp_path]).lime
    app.search_input.value_input = "superior"
    assert "Loading the lake list" in app.suggestions[0].object
    app.search_input.value_input = ""
    assert not app.suggestions.visible


def test_search_suggestions_select_with_known_info(idle_job, tmp_path, monkeypatch):
    hits = [
        {
            "hylak_id": 67,
            "name": "Great_Salt",
            "country": "United States",
            "area_km2": 4360.0,
        },
        {"hylak_id": 68, "name": "68", "country": "Canada", "area_km2": 1.5},
    ]
    monkeypatch.setattr(lake_lookup, "search", lambda text, limit=8, source=None: hits)
    app = lime_app.build_app(job=idle_job, roots=[tmp_path]).lime
    app.search_input.value_input = "sal"
    labels = [b.label for b in app.suggestions]
    assert labels[0] == "Great Salt — United States · 4,360 km² · #67"
    assert labels[1].startswith("Unnamed lake — Canada")
    app.suggestions[0].clicks += 1
    assert app.selected["hylak_id"] == 67
    assert "Great Salt" in app.lake_info.object
    assert "United States" in app.lake_info.object
    assert not app.suggestions.visible
    # Enter takes the top suggestion.
    app.search_input.value_input = "sal"
    app.search_input.param.trigger("enter_pressed")
    assert app.selected["hylak_id"] == 67


def test_select_known_lake_loads_results(idle_job):
    app = lime_app.build_app(job=idle_job, roots=[FIXTURES]).lime
    app.select_lake(999999)
    assert app.state.lake_name == "Fixture_Lake"


def test_globe_click_uses_lookup(idle_job, monkeypatch, tmp_path):
    found = {"hylak_id": 67, "name": "Great_Salt_Lake", "area_km2": 4360.0}
    monkeypatch.setattr(lake_lookup, "lookup_by_point", lambda lon, lat: found)
    app = lime_app.build_app(job=idle_job, roots=[tmp_path]).lime
    app.globe.clicked = {"lon": -112.5, "lat": 41.1}
    assert app.selected["hylak_id"] == 67
    assert "Great Salt Lake" in app.lake_info.object


def real_clay_folder(dest: Path, name: str = "Other_Lake") -> Path:
    """
    A copy of the fixture shaped like real CLAY output: no HYLAK_ID in
    config.json and no Lake_Polygon.shp (CLAY writes neither).
    """
    out = dest / name
    shutil.copytree(FIXTURE, out)
    for shp in out.glob("Lake_Polygon.*"):
        shp.unlink()
    config = json.loads((out / "config.json").read_text())
    config.pop("HYLAK_ID")
    config["LAKE_NAME"] = name
    (out / "config.json").write_text(json.dumps(config))
    return out


def test_run_clay_end_to_end(tmp_path):
    """A fake CLAY run 'writes' a new lake folder; the app opens it when done."""
    roots = tmp_path / "LakeOutputDirectory"
    roots.mkdir()
    out = tmp_path / "elsewhere" / "Fixture_Lake"
    shutil.copytree(FIXTURE, out)
    job = jobs.ClayJob(command=fake_clay_for(out))
    app = lime_app.build_app(job=job, roots=[roots]).lime
    app.select_lake(999999)  # not processed yet: nothing is loaded
    assert app.state.output_dir == ""
    app.run_clay()
    assert job.wait(timeout=30) == "done"
    app.poll_job()  # what the periodic callback does in a live page
    assert app.state.job_status == "done"
    assert "working" in app.state.job_log
    assert app.state.output_dir == str(out.resolve())
    assert any(e["output_dir"] == str(out.resolve()) for e in app._catalog)
    assert "Finished" in app.status_pill.object
    assert not app.cancel_button.visible


def test_other_pages_follow_the_job_but_keep_their_lake(tmp_path):
    """A page that did not start the run sees its status but is not hijacked."""
    out = tmp_path / "elsewhere" / "Fixture_Lake"
    shutil.copytree(FIXTURE, out)
    job = jobs.ClayJob(command=fake_clay_for(out))
    runner = lime_app.build_app(job=job, roots=[tmp_path]).lime
    viewer = lime_app.build_app(job=job, roots=[FIXTURES]).lime
    viewer.load(FIXTURE)
    runner.select_lake(999999)
    runner.run_clay()
    viewer.poll_job()
    assert viewer.run_button.disabled  # one run per server
    job.wait(timeout=30)
    viewer.poll_job()
    runner.poll_job()
    assert "Finished" in viewer.status_pill.object
    assert viewer.state.output_dir == str(FIXTURE.resolve())  # unchanged
    assert runner.state.output_dir == str(out.resolve())


def test_folder_without_id_clears_previous_selection(idle_job, tmp_path):
    """Opening real CLAY output (no HYLAK_ID) must not keep the old lake's ID."""
    other = real_clay_folder(tmp_path)
    app = lime_app.build_app(output_dir=str(FIXTURE), job=idle_job).lime
    assert app.run_button.label == "Run CLAY for #999999"
    app.load(other)
    assert app.state.hylak_id is None
    assert app.selected is None
    assert app.run_button.disabled and app.run_button.label == "Run CLAY"
    assert "Other Lake" in app.lake_info.object
    assert "unknown" in app.lake_info.object


def test_real_clay_shaped_folder_loads(idle_job, tmp_path):
    """No polygon and no ID: the app still shows data, without region caveats."""
    other = real_clay_folder(tmp_path)
    app = lime_app.build_app(output_dir=str(other), job=idle_job).lime
    assert app.state.df is not None and not app.state.polygon_geojson
    assert app.globe.lake_geojson == {}
    for i in range(len(app.tabs)):
        _render(app.tabs[i])


def test_cells_only_on_globe_while_gridded_tab_is_open(idle_job):
    app = lime_app.build_app(output_dir=str(FIXTURE), job=idle_job).lime
    assert app.tabs.active == 0
    _render(app.tabs)  # builds the Gridded tab's content
    assert app.cells.cells_geojson, "gridded tab should publish cells"
    assert app.globe.cells_geojson == {}
    assert not app.cells_caption.visible
    app.tabs.active = lime_app.GRIDDED_TAB
    assert app.globe.cells_geojson == app.cells.cells_geojson
    assert app.cells_caption.visible and "Globe cells" in app.cells_caption.object
    app.tabs.active = 0
    assert app.globe.cells_geojson == {}


def test_out_of_range_id_is_refused(idle_job):
    app = lime_app.build_app(job=idle_job).lime
    app.search_input.value_input = str(lime_app.MAX_HYLAK_ID + 1)
    app.search_input.param.trigger("enter_pressed")
    assert app.selected is None


def test_globe_pick_selects_without_lookup(idle_job, monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(
        lake_lookup, "lookup_by_id", lambda i, source=None: calls.append(i)
    )
    app = lime_app.build_app(job=idle_job, roots=[tmp_path]).lime
    app.globe.picked = {
        "hylak_id": 67,
        "name": "Great Salt",
        "country": "United States",
        "area_km2": 4360.0,
        "t": 1,
    }
    assert app.selected["name"] == "Great_Salt"
    assert app.selected["area_km2"] == 4360.0
    assert "Great Salt" in app.lake_info.object
    assert app.run_button.label == "Run CLAY for #67"
    assert calls == [67]  # the outline is fetched (in the background when live)


def test_outline_arrives_and_moves_globe(idle_job, monkeypatch, tmp_path):
    outline = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [0, 0]},
                "properties": {},
            }
        ],
    }
    monkeypatch.setattr(
        lake_lookup,
        "lookup_by_id",
        lambda i, source=None: {
            "hylak_id": i,
            "name": "From_S3",
            "area_km2": 9.0,
            "geojson": outline,
        },
    )
    app = lime_app.build_app(job=idle_job, roots=[tmp_path]).lime
    app.select_lake(5, {"name": "Known", "area_km2": None})
    assert app.globe.lake_geojson == outline
    assert app.selected["name"] == "Known"  # what we knew wins
    assert app.selected["area_km2"] == 9.0  # gaps filled from HydroLAKES
    assert "Loading outline" not in app.lake_info.object


def test_globe_gets_tiles_url(idle_job, monkeypatch):
    from GLHE.LIME.components import globe

    monkeypatch.setenv(globe.TILES_ENV, "/tiles/x.pmtiles")
    app = lime_app.build_app(job=idle_job).lime
    assert app.globe.lake_tiles_url == "/tiles/x.pmtiles"
    monkeypatch.delenv(globe.TILES_ENV)
    assert globe.lake_tiles_url() == globe.DEFAULT_TILES_URL
    monkeypatch.setenv(globe.TILES_ENV, "")
    assert globe.lake_tiles_url() == ""


def test_run_clay_failure_is_reported(tmp_path):
    job = jobs.ClayJob(command=lambda i: [sys.executable, "-c", "raise SystemExit(3)"])
    app = lime_app.build_app(job=job, roots=[tmp_path]).lime
    app.run_clay()  # nothing selected: refused
    assert job.status == "idle"
    app.select_lake(5)
    app.run_clay()
    job.wait(timeout=30)
    app.poll_job()
    assert app.state.job_status == "failed"
    assert "exit code 3" in app.log_pane.object


def test_sidebar_html_escapes():
    assert "&lt;b&gt;" in sb.log_html(["<b>"])
    assert "&lt;x&gt;" in sb.header_title_html("<x>", 1)
    assert "Running" in sb.status_pill_html("running", "#67")


def test_serve_is_exposed():
    from GLHE.LIME.app import serve

    assert callable(serve)


def test_caveats_count_reaches_the_browser_model(idle_job):
    """The Caveats tab title must update in the rendered (bokeh) model too."""
    from bokeh.document import Document

    app = lime_app.build_app(job=idle_job, roots=[FIXTURES]).lime
    root = app.tabs.get_root(Document())
    app.tabs.active = 1
    app.load(str(FIXTURE), hylak_id=999999)
    n = len(app.state.caveats)
    assert n > 0
    assert root.tabs[lime_app.CAVEATS_TAB].title == f"Caveats ({n})"
    assert app.tabs.active == 1

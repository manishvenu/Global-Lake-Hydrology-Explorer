"""Tests for the LIME backend: catalog, lake lookup and the CLAY job runner."""

import json
import sys
from pathlib import Path

import pytest

from GLHE.LIME import catalog, jobs, lake_lookup

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------- catalog
def test_catalog_finds_fixture_lake():
    lakes = catalog.list_processed_lakes(FIXTURES)
    names = [lake["name"] for lake in lakes]
    assert "Fixture_Lake" in names
    lake = lakes[names.index("Fixture_Lake")]
    assert lake["hylak_id"] == 999999
    assert Path(lake["output_dir"]) == (FIXTURES / "Fixture_Lake").resolve()


def test_catalog_handles_missing_id_bad_json_and_missing_root(tmp_path):
    (tmp_path / "A_Lake").mkdir()
    (tmp_path / "A_Lake" / "config.json").write_text(
        json.dumps({"LAKE_NAME": "A_Lake"})
    )
    (tmp_path / "Broken").mkdir()
    (tmp_path / "Broken" / "config.json").write_text("{not json")
    lakes = catalog.list_processed_lakes(tmp_path, extra_roots=[tmp_path / "nope"])
    assert lakes == [
        {
            "name": "A_Lake",
            "hylak_id": None,
            "output_dir": str((tmp_path / "A_Lake").resolve()),
        }
    ]


def test_catalog_accepts_a_single_lake_folder():
    lakes = catalog.list_processed_lakes(FIXTURES / "Fixture_Lake")
    assert [lake["name"] for lake in lakes] == ["Fixture_Lake"]


def test_catalog_default_roots_do_not_crash():
    assert isinstance(catalog.default_roots(), list)
    assert isinstance(catalog.list_processed_lakes(), list)


# ------------------------------------------------------------ lake lookup
def test_lookup_returns_none_when_data_missing(tmp_path):
    missing = tmp_path / "nope.shp"
    assert lake_lookup.lookup_by_id(67, shp=missing) is None
    assert lake_lookup.lookup_by_point(-112.5, 41.1, shp=missing) is None
    assert lake_lookup.lookup_by_id("not a number") is None


@pytest.fixture
def mini_hydrolakes(tmp_path):
    """A two-lake shapefile with HydroLAKES' column names."""
    import geopandas as gpd
    from shapely.geometry import box

    gdf = gpd.GeoDataFrame(
        {
            "Hylak_id": [67, 68],
            "Lake_name": ["Great Salt Lake", None],
            "Lake_area": [4360.0, 1.5],
        },
        geometry=[box(-112.9, 40.7, -112.0, 41.6), box(-111.0, 40.0, -110.99, 40.01)],
        crs=4326,
    )
    path = tmp_path / "mini_hydrolakes.shp"
    gdf.to_file(path)
    return path


def test_lookup_by_id(mini_hydrolakes):
    lake = lake_lookup.lookup_by_id(67, shp=mini_hydrolakes)
    assert lake["hylak_id"] == 67
    assert lake["name"] == "Great_Salt_Lake"
    assert lake["area_km2"] == pytest.approx(4360.0)
    assert lake["geojson"]["type"] == "FeatureCollection"
    assert lake_lookup.lookup_by_id(12345, shp=mini_hydrolakes) is None


def test_lookup_by_point(mini_hydrolakes):
    assert (
        lake_lookup.lookup_by_point(-112.5, 41.1, shp=mini_hydrolakes)["hylak_id"] == 67
    )
    # Just outside the small unnamed lake: nearest one is returned, name falls back to ID.
    near = lake_lookup.lookup_by_point(-110.98, 40.005, shp=mini_hydrolakes)
    assert near["hylak_id"] == 68 and near["name"] == "68"
    assert lake_lookup.lookup_by_point(0.0, 0.0, shp=mini_hydrolakes) is None


# ------------------------------------------------------------------- jobs
def fake_clay(hylak_id):
    """A fast stand-in for CLAY: prints a few lines and the output marker."""
    code = (
        f"print('working on {hylak_id}'); "
        f"print('{jobs.OUTPUT_MARKER}/tmp/lake_{hylak_id}')"
    )
    return [sys.executable, "-c", code]


def test_job_success_captures_log_and_output_dir():
    updates = []
    job = jobs.ClayJob(command=fake_clay, on_update=lambda j: updates.append(j.status))
    assert job.status == "idle"
    assert job.start(42) is True
    assert job.wait(timeout=30) == "done"
    assert job.output_dir == "/tmp/lake_42"
    assert "working on 42" in job.log
    assert updates[0] == "running" and updates[-1] == "done"


def test_job_failure():
    job = jobs.ClayJob(
        command=lambda i: [
            sys.executable,
            "-c",
            "import sys; print('boom'); sys.exit(3)",
        ]
    )
    job.start(1)
    assert job.wait(timeout=30) == "failed"
    assert job.returncode == 3 and job.output_dir is None
    assert "boom" in job.log


def test_job_missing_executable_fails_cleanly():
    job = jobs.ClayJob(command=lambda i: ["/definitely/not/a/program"])
    job.start(1)
    assert job.status == "failed"


def test_only_one_job_at_a_time_and_cancel():
    slow = lambda i: [
        sys.executable,
        "-c",
        "import time; print('start', flush=True); time.sleep(30)",
    ]
    job = jobs.ClayJob(command=slow)
    assert job.start(1) is True
    assert job.start(2) is False  # refused while running
    job.cancel()
    assert job.wait(timeout=30) == "failed"
    assert job.log[-1] == "Cancelled."
    # A new run is allowed afterwards.
    job.command = fake_clay
    assert job.start(3) is True
    assert job.wait(timeout=30) == "done"


def test_log_is_bounded():
    many = lambda i: [sys.executable, "-c", "for n in range(500): print(n)"]
    job = jobs.ClayJob(command=many, max_log_lines=50)
    job.start(1)
    job.wait(timeout=30)
    assert len(job.log) == 50
    assert job.log[-1] == "CLAY finished."


def test_default_command_uses_run_clay():
    argv = jobs.default_command(67)
    assert argv[0] == sys.executable and argv[-2:] == ["GLHE.LIME.run_clay", "67"]


def test_job_survives_non_utf8_output():
    """A byte that is not valid UTF-8 must not leave the job stuck in "running"."""
    code = "import sys; sys.stdout.buffer.write(b'caf\\xe9\\n'); sys.stdout.flush()"
    job = jobs.ClayJob(command=lambda i: [sys.executable, "-c", code])
    job.start(1)
    assert job.wait(timeout=30) == "done"
    assert any(line.startswith("caf") for line in job.log)


def test_job_reader_error_marks_failed(monkeypatch):
    """Any exception in the reader thread ends the job as "failed"."""
    job = jobs.ClayJob(command=fake_clay)

    def broken_append(line):
        if line == "working on 5":
            raise RuntimeError("boom")
        jobs.ClayJob._append(job, line)

    monkeypatch.setattr(job, "_append", broken_append)
    job.start(5)
    assert job.wait(timeout=30) == "failed"
    assert not job.is_running


def test_resolve_path_handles_windows_paths(tmp_path):
    """Config files copied from Windows use backslashes; find the file by name."""
    from GLHE.LIME.state import resolve_path

    (tmp_path / "X_Data.csv").write_text("Date\n")
    win = r"C:\Users\me\LakeOutputDirectory\X\X_Data.csv"
    assert resolve_path(win, tmp_path) == tmp_path / "X_Data.csv"
    posix = "/somewhere/else/X_Data.csv"
    assert resolve_path(posix, tmp_path) == tmp_path / "X_Data.csv"

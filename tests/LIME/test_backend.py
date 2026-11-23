"""Tests for the LIME backend: catalog, lake search and the CLAY job runner."""

import json
import sys
from pathlib import Path

import pytest

from GLHE.CLAY import hydrolakes
from GLHE.LIME import catalog, jobs
from GLHE.LIME.app import find_file

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------- catalog
def test_catalog_finds_fixture_lake():
    lakes = catalog.list_processed_lakes(FIXTURES)
    names = [lake["name"] for lake in lakes]
    assert "Fixture_Lake" in names
    lake = lakes[names.index("Fixture_Lake")]
    assert lake["hylak_id"] == 999999
    assert Path(lake["output_dir"]) == (FIXTURES / "Fixture_Lake").resolve()


def test_catalog_skips_broken_folders(tmp_path):
    (tmp_path / "A_Lake").mkdir()
    (tmp_path / "A_Lake" / "config.json").write_text(json.dumps({"LAKE_NAME": "A"}))
    (tmp_path / "Broken").mkdir()
    (tmp_path / "Broken" / "config.json").write_text("{not json")
    lakes = catalog.list_processed_lakes(tmp_path)
    assert lakes == [
        {
            "name": "A",
            "hylak_id": None,
            "output_dir": str((tmp_path / "A_Lake").resolve()),
        }
    ]
    assert catalog.list_processed_lakes(tmp_path / "missing") == []


@pytest.fixture
def mini_hydrolakes(tmp_path):
    """A three-lake HydroLAKES GeoParquet file (same layout as the real one)."""
    import geopandas as gpd
    from shapely.geometry import box

    gdf = gpd.GeoDataFrame(
        {
            "Hylak_id": [67, 68, 69],
            "Lake_name": ["Great Salt Lake", None, "Lac Léman"],
            "Country": ["United States", "United States", "Switzerland"],
            "Lake_area": [4360.0, 1.5, 580.0],
        },
        geometry=[
            box(-112.9, 40.7, -112.0, 41.6),
            box(-111.0, 40.0, -110.99, 40.01),
            box(6.1, 46.2, 6.9, 46.5),
        ],
        crs=4326,
    )
    path = tmp_path / "mini_hydrolakes.parquet"
    hydrolakes.write_parquet(gdf, path)
    return str(path)


# ------------------------------------------------------------------- jobs
def fake_clay(hylak_id):
    """A fast stand-in for CLAY: prints a few lines and the output marker."""
    code = (
        f"print('working on {hylak_id}'); "
        f"print('{jobs.OUTPUT_MARKER}/tmp/lake_{hylak_id}')"
    )
    return [sys.executable, "-c", code]


def test_job_success_captures_log_and_output_dir(wait):
    job = jobs.ClayJob(command=fake_clay)
    assert job.status == "idle"
    assert job.start(42) is True
    assert wait(job) == "done"
    assert job.output_dir == "/tmp/lake_42"
    assert "working on 42" in job.log


def test_job_failure(wait):
    code = "import sys; print('boom'); sys.exit(3)"
    job = jobs.ClayJob(command=lambda i: [sys.executable, "-c", code])
    job.start(1)
    assert wait(job) == "failed"
    assert job.output_dir is None and "boom" in job.log


def test_job_missing_executable_fails_cleanly(wait):
    job = jobs.ClayJob(command=lambda i: ["/definitely/not/a/program"])
    assert job.start(1) and job.status == "failed"
    assert "Could not start CLAY" in job.log[-1]


def test_only_one_job_at_a_time_and_cancel(wait):
    slow = "import time; print('start', flush=True); time.sleep(30)"
    job = jobs.ClayJob(command=lambda i: [sys.executable, "-c", slow])
    assert job.start(1) is True
    assert job.start(2) is False  # refused while running
    job.cancel()
    assert wait(job) == "failed"
    # A new run is allowed afterwards.
    job.command = fake_clay
    assert job.start(3) is True
    assert wait(job) == "done"


def test_default_command_uses_run_clay():
    argv = jobs.default_command(67)
    assert argv[0] == sys.executable and argv[-2:] == ["GLHE.LIME.run_clay", "67"]


def test_job_survives_non_utf8_output(wait):
    code = "import sys; sys.stdout.buffer.write(b'caf\\xe9\\n'); sys.stdout.flush()"
    job = jobs.ClayJob(command=lambda i: [sys.executable, "-c", code])
    job.start(1)
    assert wait(job) == "done"
    assert any(line.startswith("caf") for line in job.log)


def test_find_file_handles_windows_and_moved_paths(tmp_path):
    """Config files copied from Windows use backslashes; find the file by name."""
    (tmp_path / "X_Data.csv").write_text("Date\n")
    win = r"C:\Users\me\LakeOutputDirectory\X\X_Data.csv"
    assert find_file(win, tmp_path) == tmp_path / "X_Data.csv"
    posix = "/somewhere/else/X_Data.csv"
    assert find_file(posix, tmp_path) == tmp_path / "X_Data.csv"


# ------------------------------------------------------------- lake search
def test_get_lake_reads_one_row_group_and_caches(mini_hydrolakes, monkeypatch):
    pf = hydrolakes._parquet_file(mini_hydrolakes)
    # IDs 67-69 are in row group 0 of this one-group file.
    assert hydrolakes._row_group_for(pf, 67) == 0
    assert hydrolakes._row_group_for(pf, 12345) is None
    lake = hydrolakes.get_lake(69, mini_hydrolakes)
    assert lake["Lake_name"].iloc[0] == "Lac Léman"
    # Cached: a second call does not read the file again.
    monkeypatch.setattr(pf, "read_row_group", None)
    assert hydrolakes.get_lake(69, mini_hydrolakes)["Hylak_id"].iloc[0] == 69
    # Unknown IDs fall back to the filter path and give None.
    assert hydrolakes.get_lake(5_000_000, mini_hydrolakes) is None


def test_search_by_name_country_and_id(mini_hydrolakes):
    def search(text):
        return hydrolakes.search(text, src=mini_hydrolakes)

    hit = search("LÉMAN")
    assert [r["hylak_id"] for r in hit] == [69]
    assert hit[0]["name"] == "Lac Léman" and hit[0]["country"] == "Switzerland"
    # "great" is a name prefix; "salt" is a word inside a name.
    assert search("great")[0]["hylak_id"] == 67
    assert search("salt")[0]["hylak_id"] == 67
    # Country: largest lakes first, unnamed ones included.
    usa = search("united")
    assert [r["hylak_id"] for r in usa] == [67, 68]
    assert usa[1]["name"] is None
    # IDs.
    assert search("#68")[0]["area_km2"] == 1.5
    assert search("12345") == []
    assert search("zzz") == []


def test_search_limit(mini_hydrolakes):
    assert len(hydrolakes.search("united", limit=1, src=mini_hydrolakes)) == 1
    assert hydrolakes.normalize("  Lac_LÉMAN ") == "lac leman"

"""Public mode guards (GLHE_PUBLIC=1)."""

import sys
from pathlib import Path

import panel as pn
import pytest

from GLHE.LIME import app as lime_app
from GLHE.LIME import jobs, public

FIXTURES = Path(__file__).parent / "fixtures"
FIXTURE = FIXTURES / "Fixture_Lake"


@pytest.fixture
def public_mode(monkeypatch):
    monkeypatch.setenv("GLHE_PUBLIC", "1")
    pn.state.cache.pop("glhe_public_last_run", None)
    yield
    pn.state.cache.pop("glhe_public_last_run", None)


def slow_job(**kw):
    """A job whose fake CLAY sleeps, so it is still running when we look."""
    cmd = lambda _id: [sys.executable, "-c", "import time; time.sleep(30)"]
    return jobs.ClayJob(command=cmd, **kw)


def test_public_off_by_default(monkeypatch):
    monkeypatch.delenv("GLHE_PUBLIC", raising=False)
    assert not public.enabled()
    assert public.max_run_seconds() is None


def test_limits_from_env(public_mode, monkeypatch):
    monkeypatch.setenv("GLHE_MAX_RUN_MINUTES", "2")
    assert public.max_run_seconds() == 120


def test_lake_block_reasons(public_mode):
    assert public.lake_block_reason({"hylak_id": 5, "area_km2": 50}, set()) is None
    assert public.lake_block_reason({"hylak_id": 5, "area_km2": 50}, {5})
    assert "unknown" in public.lake_block_reason({"hylak_id": 5}, set())
    assert "large" in public.lake_block_reason({"hylak_id": 5, "area_km2": 1e6}, set())


def test_cooldown(public_mode):
    public.record_run("1.2.3.4", now=1000)
    assert public.visitor_block_reason("1.2.3.4", now=1060)
    assert public.visitor_block_reason("1.2.3.4", now=1000 + 11 * 60) is None
    assert public.visitor_block_reason("5.6.7.8", now=1060) is None


def test_time_limit_stops_job():
    job = slow_job(max_seconds=0.5)
    assert job.start(1)
    assert job.wait(15) == "failed"
    assert any("Time limit reached" in line for line in job.log)


def test_app_blocks_processed_and_unknown_size_lakes(public_mode, tmp_path):
    job = slow_job()
    app = lime_app.build_app(job=job, roots=[FIXTURES]).lime
    app.select_lake(999999, {"hylak_id": 999999, "area_km2": 50.0})  # the fixture lake
    assert app.run_button.disabled and app.run_button.label == "Already processed"
    app.select_lake(1234, {"hylak_id": 1234})
    assert app.run_button.disabled and "unknown" in app.run_button.label
    app.select_lake(1235, {"hylak_id": 1235, "area_km2": 20.0})
    assert not app.run_button.disabled
    assert public.BANNER in [getattr(o, "object", None) for o in app.main()]


def test_app_cooldown_and_cancel_only_by_starter(public_mode):
    job = slow_job()
    try:
        a = lime_app.build_app(job=job, roots=[FIXTURES]).lime
        b = lime_app.build_app(job=job, roots=[FIXTURES]).lime
        lake = {"hylak_id": 1235, "area_km2": 20.0}
        a.select_lake(1235, dict(lake))
        a.run_clay()
        assert job.is_running and a.cancel_button.visible
        b.poll_job()
        assert not b.cancel_button.visible  # another visitor cannot cancel
        b.cancel_clay()
        assert job.is_running
        job.cancel()
        job.wait(15)
        a.poll_job()
        a.run_clay()  # same visitor, inside the cooldown
        assert not job.is_running
    finally:
        job.cancel()


def test_visitor_id_prefers_cloudflare_header(monkeypatch):
    from GLHE.LIME import public

    headers = {"X-Forwarded-For": "1.1.1.1, 10.0.0.1"}
    monkeypatch.setattr(type(pn.state), "headers", property(lambda s: headers))
    assert public.visitor_id() == "1.1.1.1"
    headers["CF-Connecting-IP"] = "2.2.2.2"
    assert public.visitor_id() == "2.2.2.2"

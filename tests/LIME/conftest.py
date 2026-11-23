"""Shared fixtures for the LIME tests."""

import time

import pytest


@pytest.fixture(autouse=True)
def no_network_hydrolakes(monkeypatch, tmp_path):
    """Keep tests off the network: by default HydroLAKES points at a missing
    local file. Tests that need lakes pass their own ``source``."""
    monkeypatch.setenv("GLHE_HYDROLAKES", str(tmp_path / "no_hydrolakes.parquet"))


@pytest.fixture
def wait():
    """``wait(job)``: block until a ClayJob stops running; returns its status."""

    def _wait(job, timeout=30):
        end = time.time() + timeout
        while job.is_running and time.time() < end:
            time.sleep(0.05)
        return job.status

    return _wait

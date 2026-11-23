"""Shared fixtures for the LIME tests."""

import pytest


@pytest.fixture(autouse=True)
def no_network_hydrolakes(monkeypatch, tmp_path):
    """Keep tests off the network: by default HydroLAKES points at a missing
    local file. Tests that need lakes pass their own ``source``."""
    monkeypatch.setenv("GLHE_HYDROLAKES", str(tmp_path / "no_hydrolakes.parquet"))

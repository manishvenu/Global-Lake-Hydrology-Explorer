"""Shared fixtures for the CLAY tests."""

import pytest

from GLHE.CLAY import data_sources


@pytest.fixture(autouse=True)
def no_data_server(monkeypatch):
    """Tests never reach the real GLHE data server unless they set
    GLHE_DATA_URL themselves (see test_data_sources.py)."""
    monkeypatch.setenv(data_sources.SERVER_ENV, "")
    data_sources._checked.clear()

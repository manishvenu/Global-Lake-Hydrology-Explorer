"""data_check must not crash on optional local-only products (e.g. GLEAM)."""

from GLHE.CLAY.data_access import data_check


def test_local_only_product_without_files_is_skipped(monkeypatch, tmp_path):
    monkeypatch.setattr(data_check, "_LOCAL_DATA_DIR", tmp_path)
    monkeypatch.setattr(
        data_check,
        "_load_products",
        lambda: [{"name": "GLEAM", "access_code": "local", "local_path": "GLEAM"}],
    )
    data_check.check_data_and_download_missing_data_or_files()  # no KeyError
    data_check.download_for_offline()  # nothing to download, no KeyError

# Subpackages are loaded lazily so importing GLHE (or GLHE.CALCITE) does not
# pull in CLAY's heavy dependencies (pandas, xarray, geopandas, …) at import
# time. Access GLHE.CLAY / GLHE.LIME / GLHE.CALCITE as usual — the first
# attribute access triggers the real import.


def __getattr__(name: str):
    if name in ("CLAY", "LIME", "CALCITE"):
        import importlib

        module = importlib.import_module(f".{name}", __name__)
        globals()[name] = module  # cache so __getattr__ is only called once
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

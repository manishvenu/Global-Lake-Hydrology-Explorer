"""
A gridded data product described entirely by a spec, so most new
precipitation or evaporation datasets need no Python class of their own.

Add an entry to ``GLHE/CLAY/config/data_products.json``::

    "CHIRPS": {
      "module_name": "gridded_product",
      "class_name": "GriddedProduct",
      "run_on_start": true,
      "loaded": false,
      "Product_README": "CHIRPS v3 monthly precipitation (CC BY 4.0) ...",
      "spec": {
        "product_name": "CHIRPS",
        "local_path": "zarr/CHIRPS_v3_monthly.zarr",
        "variables": {"precip": "p"},
        "units": {"precip": "mm/month"}
      }
    }

Spec keys
---------
product_name : str
    Name used in outputs (CSV column ``p.CHIRPS``, plots, gridded zip).
local_path : str
    Zarr store or NetCDF file(s) under ``GLHE/CLAY/LocalData/`` (on the
    server: ``~/glhe-data/LocalData``). A glob such as ``"IMERG/*.nc"`` works
    for many NetCDF files.
cloud : str, optional
    The Zarr store's path on the GLHE data server and in the ``glhe`` S3
    bucket (e.g. ``"zarr/CRUTS.zarr"``), used when the local copy is missing:
    ``https://glhe.venumuddula.com/<cloud>``, then ``s3://glhe/<cloud>``.
variables : dict
    ``{name in the file: single-letter code}``. Only ``"p"`` and ``"e"`` are
    supported (they are converted to mm/month); inflow and outflow need a
    catchment, not a lake-area average.
units : dict, optional
    ``{name in the file: unit}`` for files whose ``units`` attribute is
    missing or not understood by pint (e.g. ``"mm/day"``, ``"kg m-2 s-1"``).

What it does: open the data, subset it to the lake's bounding box (with one
cell of padding, or a 0.5 degree buffer for lakes smaller than a cell),
convert to mm/month, average over the box and put each value on the first
of its month. If the data is missing it raises, and CLAY skips the product.
"""

from __future__ import annotations

import glob
from pathlib import Path

import xarray as xr

from GLHE.CALCITE import events
from GLHE.CLAY import data_sources, lake_extraction, xarray_helpers
from GLHE.CLAY.data_access import data_access_parent_class, data_check
from GLHE.CLAY.helpers import MVSeries

#: The variable name each single-letter code is renamed to; these names are
#: in GLHE.CLAY.globals.SLC_MAPPING, so the MVSeries get the right code.
STANDARD_NAMES = {"p": "precip", "e": "evap"}
OUTPUT_UNIT = "mm/month"


class GriddedProduct(data_access_parent_class.DataAccess):
    """A gridded p/e product configured by a spec (see the module docstring)."""

    def __init__(self, spec: dict):
        self.spec = spec
        self.product_name = spec.get("product_name", "GriddedProduct")
        self.README_default_information = (
            "Validate this data with the gridded geodata in the zip file"
        )
        super().__init__()

    def verify_inputs(self) -> bool:
        """The spec has what is needed and only uses supported codes."""
        missing = [
            k for k in ("product_name", "local_path", "variables") if k not in self.spec
        ]
        if missing:
            self.logger.error("%s spec is missing %s", self.product_name, missing)
            return False
        bad = {
            v: c for v, c in self.spec["variables"].items() if c not in STANDARD_NAMES
        }
        if bad:
            self.logger.error(
                "%s: unsupported codes %s (only %s)",
                self.product_name,
                bad,
                sorted(STANDARD_NAMES),
            )
            return False
        return True

    def attach_geodata(self) -> str:
        return "grid"

    # ------------------------------------------------------------- reading
    def local_path(self) -> Path:
        return Path(data_check._LOCAL_DATA_DIR) / self.spec["local_path"]

    def open_dataset(self) -> xr.Dataset:
        """The product's data: local NetCDF files, or a Zarr store from this
        machine, else the GLHE data server, else S3 (see data_sources)."""
        path = self.local_path()
        if path.suffix != ".zarr":
            files = sorted(glob.glob(str(path)))
            if files:
                self.logger.info(
                    "Reading %s from %d file(s)", self.product_name, len(files)
                )
                return xr.open_mfdataset(files, combine="by_coords")
        if path.suffix == ".zarr" or self.spec.get("cloud"):
            local = path if path.suffix == ".zarr" else None
            return data_sources.open_zarr(self.spec.get("cloud"), local)
        raise FileNotFoundError(f"{self.product_name} data not found at {path}")

    def prepare(self, dataset: xr.Dataset) -> xr.Dataset:
        """Standard names (time/lat/lon, precip/evap) and units on the variables."""
        if "valid_time" in dataset.dims and "time" not in dataset.dims:
            dataset = dataset.rename({"valid_time": "time"})
        dataset = xarray_helpers.label_xarray_dataset_with_product_name(
            dataset, self.product_name
        )
        dataset = xarray_helpers.fix_lat_long_names_in_xarray_dataset(dataset)
        renames = {v: STANDARD_NAMES[c] for v, c in self.spec["variables"].items()}
        dataset = dataset[list(renames)].rename(renames)
        for var, unit in self.spec.get("units", {}).items():
            dataset[renames[var]].attrs["units"] = unit
        return dataset

    # --------------------------------------------------------------- driver
    def product_driver(self, polygon, debug=False, run_cleanly=False) -> list[MVSeries]:
        """See parent class for description."""
        self.logger.info("%s driver started", self.product_name)
        dataset = self.prepare(self.open_dataset())
        if float(dataset.lon.max()) > 180:  # 0..360 grid -> -180..180
            dataset = dataset.assign_coords(lon=(dataset.lon + 180) % 360 - 180).sortby(
                "lon"
            )
        try:
            dataset = lake_extraction.subset_box(dataset, polygon, 1)
        except ValueError:  # lake smaller than one grid cell
            dataset = lake_extraction.subset_box(dataset, polygon.buffer(0.5), 0)
        names = list(STANDARD_NAMES[c] for c in self.spec["variables"].values())
        dataset = xarray_helpers.convert_xarray_dataset_units(
            dataset.load(), OUTPUT_UNIT, *names
        )
        series = list(
            xarray_helpers.spatially_average_xarray_dataset_and_convert(dataset, *names)
        )
        for s in series:  # monthly values sit on the 1st of their month
            s.dataset.index = s.dataset.index.to_period("M").to_timestamp()
        self.send_data_product_event(
            events.DataProductRunEvent(
                self.product_name, self.README_default_information
            )
        )
        self.logger.info("%s driver finished", self.product_name)
        return series

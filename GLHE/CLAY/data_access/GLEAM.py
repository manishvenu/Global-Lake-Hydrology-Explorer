"""GLEAM actual evaporation data access.

GLEAM (Global Land Evaporation Amsterdam Model) v3 provides globally gridded
actual evaporation (Ea) and open-water evaporation (Ew) at 0.25° resolution,
1980-present.

Data must be downloaded from https://www.gleam.eu — free registration required.
Place the downloaded NetCDF files in:

    GLHE/CLAY/LocalData/GLEAM/

Preferred variable: Ew (open-water evaporation, most physically appropriate
for a lake).  Falls back to Ea (total actual evaporation) if Ew files are
absent.

File naming conventions supported:
    Ew_YYYY_GLEAM_v3.Xa.nc  (or v3.Xb)
    Ea_YYYY_GLEAM_v3.Xa.nc
    monthly/Ew_1980-2023_GLEAM_v3.Xa.nc  (annual concatenated file)
"""

import os
import numpy as np
import pandas as pd
import xarray as xr
from calendar import monthrange
from pathlib import Path
from shapely.geometry import Polygon

from GLHE.CALCITE import events
from GLHE.CLAY import lake_extraction, helpers, xarray_helpers
from GLHE.CLAY.data_access import data_access_parent_class
from GLHE.CLAY.helpers import MVSeries

_GLEAM_DIR = Path(__file__).parent.parent / "LocalData" / "GLEAM"
_GLEAM_REGISTER_URL = "https://www.gleam.eu"


class GLEAM(data_access_parent_class.DataAccess):
    """Actual lake evaporation from GLEAM v3 (local NetCDF files).

    Downloads are not automated — see module docstring for setup instructions.
    product_driver raises RuntimeError with a clear message if no files are
    found; CLAY catches this and continues with the other products.
    """

    def __init__(self):
        self.README_default_information = (
            "Actual lake evaporation (mm/month) from GLEAM v3 "
            "(open-water variable Ew, or total Ea if Ew unavailable). "
            "0.25° resolution, 1980-present. "
            "Download from %s and place files in LocalData/GLEAM/." % _GLEAM_REGISTER_URL
        )
        super().__init__()

    def verify_inputs(self) -> bool:
        self.logger.info("Verifying inputs: GLEAM")
        files = self._find_files()
        if not files:
            self.logger.warning(
                "No GLEAM files found in %s. "
                "Download Ew or Ea NetCDF files from %s and place them there. "
                "GLEAM data will be skipped for this run.",
                _GLEAM_DIR,
                _GLEAM_REGISTER_URL,
            )
        return True

    def attach_geodata(self) -> str:
        self.logger.info("Attaching Geo Data: GLEAM (spatially averaged)")
        return "grid"

    def product_driver(self, polygon, debug=False, run_cleanly=False) -> list[MVSeries]:
        self.logger.info("GLEAM Driver Started: Evaporation")

        files = self._find_files()
        if not files:
            raise RuntimeError(
                "No GLEAM NetCDF files found in %s. "
                "Download Ew_*_GLEAM_*.nc or Ea_*_GLEAM_*.nc from %s "
                "(free registration required) and place them there."
                % (_GLEAM_DIR, _GLEAM_REGISTER_URL)
            )

        if not run_cleanly and not debug:
            try:
                dataset = helpers.unpickle_var("GLEAM")
                self.logger.info("Using cached GLEAM data")
            except FileNotFoundError:
                dataset = self._load_and_process(files, polygon)
        else:
            dataset = self._load_and_process(files, polygon)

        var_name = [v for v in dataset.data_vars][0]
        (evap_ds,) = xarray_helpers.spatially_average_xarray_dataset_and_convert(
            dataset, var_name
        )
        # Force SLC to "e" regardless of variable name
        evap_ds.single_letter_code = "e"
        evap_ds.product_name = "GLEAM"

        self.send_data_product_event(
            events.DataProductRunEvent("GLEAM", self.README_default_information)
        )
        self.logger.info("GLEAM Driver Finished")
        return [evap_ds]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_files(self) -> list[Path]:
        """Return GLEAM NetCDF files, preferring open-water Ew over total Ea."""
        if not _GLEAM_DIR.exists():
            return []
        ew_files = sorted(_GLEAM_DIR.glob("Ew_*_GLEAM_*.nc"))
        if ew_files:
            self.logger.info("Found %d GLEAM Ew (open-water) files", len(ew_files))
            return ew_files
        ea_files = sorted(_GLEAM_DIR.glob("Ea_*_GLEAM_*.nc"))
        if ea_files:
            self.logger.info("Found %d GLEAM Ea (actual ET) files", len(ea_files))
            return ea_files
        # Also try single-file concatenated downloads
        for pattern in ("Ew_*.nc", "Ea_*.nc"):
            matches = sorted(_GLEAM_DIR.glob(pattern))
            if matches:
                return matches
        return []

    def _load_and_process(self, files: list[Path], polygon: Polygon) -> xr.Dataset:
        self.logger.info("Loading GLEAM files: %s ... (%d total)", files[0].name, len(files))
        ds = xr.open_mfdataset([str(f) for f in files], combine="by_coords")

        # Identify variable (Ew or Ea)
        var_name = next((v for v in ("Ew", "Ea") if v in ds.data_vars), list(ds.data_vars)[0])
        ds = ds[[var_name]]

        ds = xarray_helpers.label_xarray_dataset_with_product_name(ds, "GLEAM")
        ds = xarray_helpers.fix_lat_long_names_in_xarray_dataset(ds)

        # Clip to lake bounding box (GLEAM uses -180..180 longitudes)
        try:
            ds = lake_extraction.subset_box(ds, polygon, 1, long_type_180=True)
        except (ValueError, KeyError):
            self.logger.warning("GLEAM subset failed — trying with buffered polygon")
            ds = lake_extraction.subset_box(ds, polygon.buffer(0.5), 0, long_type_180=True)

        # Resample to monthly if data is daily or sub-monthly
        time_step = pd.infer_freq(pd.DatetimeIndex(ds.time.values[:24]))
        if time_step is not None and time_step.startswith(("D", "H", "T")):
            ds = ds.resample(time="MS").mean()

        # GLEAM units are mm/day; convert to mm/month
        if "units" not in ds[var_name].attrs or ds[var_name].attrs.get("units", "").startswith("mm"):
            ds[var_name].attrs["units"] = "mm/day"
            days_in_month = np.array(
                [monthrange(int(t.dt.year), int(t.dt.month))[1] for t in ds.time],
                dtype=float,
            )
            ds[var_name] = ds[var_name] * xr.DataArray(days_in_month, dims=["time"])
            ds[var_name].attrs["units"] = "mm/month"

        helpers.pickle_var(ds, "GLEAM")
        return ds


if __name__ == "__main__":
    print("This is the GLEAM module, not a script")

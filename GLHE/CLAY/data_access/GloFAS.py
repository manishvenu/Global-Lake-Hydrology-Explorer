"""GloFAS global river discharge data access.

GloFAS (Global Flood Awareness System) v4 ERA5-based reanalysis provides
monthly-mean river discharge at 0.05° resolution globally, 1979–2022.

Data is accessed via the Copernicus Climate Data Store (CDS) using the same
cdsapi credentials as ERA5-Land (~/.cdsapirc).

Register at https://cds.climate.copernicus.eu/

Reading the downloaded GRIB2 files requires cfgrib:
    conda install -c conda-forge cfgrib
"""

import os
import cdsapi
import numpy as np
import pandas as pd
import xarray as xr
from calendar import monthrange
from pathlib import Path
from shapely.geometry import Polygon

from GLHE.CALCITE import events
from GLHE.CLAY import helpers
from GLHE.CLAY.data_access import data_access_parent_class
from GLHE.CLAY.helpers import MVSeries

_TEMP_FILE = ".temp/TEMPORARY_GloFAS_DONOTOPEN.grib"
_MAX_DISTANCE_DEG = 1.0


class GloFAS(data_access_parent_class.DataAccess):
    """River discharge from GloFAS v4 ERA5-based reanalysis via CDS API.

    Uses the same CDS API key as ERA5-Land.  Reads ~0.05° gridded monthly
    discharge and extracts the highest-discharge pixel within the lake bounding
    box as a proxy for the main river channel at the lake outlet.

    Returns discharge as m³/month (monthly volume) so it is directly
    comparable with NWM inflow/outflow.
    """

    def __init__(self):
        self.README_default_information = (
            "River discharge (m³/month) from GloFAS v4 ERA5-based reanalysis "
            "(Lisflood hydrological model, 0.05° resolution, 1979–2022). "
            "Uses the same CDS API credentials as ERA5-Land (~/.cdsapirc). "
            "Register at https://cds.climate.copernicus.eu/"
        )
        self.api_client = cdsapi.Client()
        super().__init__()

    def verify_inputs(self) -> bool:
        self.logger.info("Verifying inputs: GloFAS")
        return True

    def attach_geodata(self) -> str:
        self.logger.info("Attaching Geo Data: GloFAS (point observation, no grid)")
        return "complete"

    def product_driver(self, polygon, debug=False, run_cleanly=False) -> list[MVSeries]:
        self.logger.info("GloFAS Driver Started: River Discharge")

        if not run_cleanly and not debug:
            try:
                series = helpers.unpickle_var("GloFAS")
                self.logger.info("Using cached GloFAS data")
            except FileNotFoundError:
                series = self._fetch_and_process(polygon)
        else:
            series = self._fetch_and_process(polygon)

        mv = MVSeries(
            dataset=series,
            unit="m^3/month",
            single_letter_code="i",
            product_name="GloFAS",
            variable_name="dis24",
            xarr_dataset=None,
        )
        self.send_data_product_event(
            events.DataProductRunEvent("GloFAS", self.README_default_information)
        )
        self.logger.info("GloFAS Driver Finished")
        return [mv]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_and_process(self, polygon: Polygon) -> pd.Series:
        bounds = polygon.bounds  # (min_lon, min_lat, max_lon, max_lat)
        pad = 1.0

        self.logger.info("Downloading GloFAS reanalysis via CDS API (this may take a while)")
        self.api_client.retrieve(
            "cems-glofas-reanalysis",
            {
                "system_version": "version_4_0",
                "hydrological_model": "lisflood",
                "product_type": "consolidated",
                "variable": "river_discharge_in_the_last_24_hours",
                "temporal_resolution": "monthly",
                "year": [str(y) for y in range(1979, 2023)],
                "month": [f"{m:02d}" for m in range(1, 13)],
                "area": [
                    bounds[3] + pad,  # N
                    bounds[0] - pad,  # W
                    bounds[1] - pad,  # S
                    bounds[2] + pad,  # E
                ],
                "format": "grib",
            },
            _TEMP_FILE,
        )

        ds = self._open_grib(_TEMP_FILE)

        centroid_lat = polygon.centroid.y
        centroid_lon = polygon.centroid.x

        lat_name = "latitude" if "latitude" in ds.coords else "lat"
        lon_name = "longitude" if "longitude" in ds.coords else "lon"
        lats = ds[lat_name].values
        lons = ds[lon_name].values

        lon_grid, lat_grid = np.meshgrid(lons, lats)
        dist = np.hypot(lon_grid - centroid_lon, lat_grid - centroid_lat)

        var_name = [v for v in ds.data_vars][0]
        max_dis = ds[var_name].max(dim="time").values

        # Within the distance threshold, pick the pixel with highest max discharge
        # (proxy for main river channel rather than overland flow pixels).
        mask = dist < _MAX_DISTANCE_DEG
        if not mask.any():
            raise RuntimeError(
                "No GloFAS pixel within %.1f° of lake centroid (%.4f, %.4f). "
                "Lake may be outside GloFAS domain." % (_MAX_DISTANCE_DEG, centroid_lat, centroid_lon)
            )

        max_dis_masked = np.where(mask, max_dis, 0.0)
        lat_idx, lon_idx = np.unravel_index(int(max_dis_masked.argmax()), max_dis_masked.shape)
        sel_lat = float(lats[lat_idx])
        sel_lon = float(lons[lon_idx])
        self.logger.info(
            "GloFAS: selected pixel at (%.4f, %.4f), distance=%.4f°, peak_discharge=%.2f m³/s",
            sel_lat, sel_lon, dist[lat_idx, lon_idx], float(max_dis[lat_idx, lon_idx]),
        )

        time_dim = "time" if "time" in ds.dims else "valid_time"
        series = (
            ds[var_name]
            .isel(**{lat_name: lat_idx, lon_name: lon_idx})
            .to_series()
        )

        series.index = pd.DatetimeIndex(series.index).to_period("M").to_timestamp()
        series = series.groupby(series.index).mean()
        series.name = "dis24"

        # Convert monthly mean m³/s → m³/month
        days = series.index.map(lambda t: monthrange(t.year, t.month)[1])
        series = series * days * 86400

        helpers.pickle_var(series, "GloFAS")
        helpers.clean_up_specific_temporary_files("GloFAS")
        return series

    def _open_grib(self, path: str) -> xr.Dataset:
        try:
            import cfgrib  # noqa: F401 — validates cfgrib is installed
            return xr.open_dataset(path, engine="cfgrib")
        except ImportError:
            raise RuntimeError(
                "Reading GloFAS GRIB files requires cfgrib. "
                "Install with: conda install -c conda-forge cfgrib"
            )
        except Exception as e:
            # cfgrib may need filter_by_keys to disambiguate multiple GRIB messages
            try:
                return xr.open_dataset(
                    path,
                    engine="cfgrib",
                    backend_kwargs={"filter_by_keys": {"cfVarName": "dis24"}},
                )
            except Exception:
                raise RuntimeError(
                    "Failed to read GloFAS GRIB file: %s. "
                    "Ensure cfgrib is installed and the file is valid." % e
                )


if __name__ == "__main__":
    print("This is the GloFAS module, not a script")

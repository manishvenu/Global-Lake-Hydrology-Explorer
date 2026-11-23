"""DAHITI water level data access.

DAHITI (Database for Hydrological Time Series of Inland Waters) provides
satellite-altimetry-derived water surface elevation time series for lakes and
reservoirs worldwide.  https://dahiti.dgfi.tum.de/en/

Credentials
-----------
DAHITI v2 (preferred) — set one environment variable:

    export DAHITI_API_KEY=your_api_key_here

DAHITI v1 (fallback) — set two environment variables:

    export DAHITI_USERNAME=your_username
    export DAHITI_PASSWORD=your_password

The class prefers v2 when DAHITI_API_KEY is set.  If only v1 credentials are
present it falls back to the username/password POST interface.

Register for a free account at https://dahiti.dgfi.tum.de/en/.

Lakes outside DAHITI coverage are skipped gracefully — CLAY continues with the
other data products.
"""

import os
import numpy as np
import pandas as pd
import requests
from shapely.geometry import Polygon

import GLHE.CLAY.globals
from GLHE.CALCITE import pubsub, events
from GLHE.CLAY import helpers
from GLHE.CLAY.data_access import data_access_parent_class
from GLHE.CLAY.helpers import MVSeries

_API_V2 = "https://dahiti.dgfi.tum.de/api/v2/"
_API_V1 = "https://dahiti.dgfi.tum.de/api/v1/"

# Closest acceptable DAHITI target distance.  Beyond this the lake is likely
# outside DAHITI coverage (~55 km at mid-latitudes).
_MAX_DISTANCE_DEG = 0.5


class DAHITI(data_access_parent_class.DataAccess):
    """Water surface elevation from DAHITI satellite altimetry.

    Reads credentials from environment variables at construction time:
      - DAHITI_API_KEY  → uses the v2 REST API (preferred)
      - DAHITI_USERNAME + DAHITI_PASSWORD  → falls back to v1 POST API

    A missing key does not abort the CLAY run; product_driver raises so CLAY
    logs a warning and continues with the other data products.
    """

    _api_key: str | None
    _username: str | None
    _password: str | None

    def __init__(self):
        self.README_default_information = (
            "Water surface elevation (m, WGS84 ellipsoidal heights) from DAHITI "
            "satellite altimetry. "
            "Set DAHITI_API_KEY (v2) or DAHITI_USERNAME + DAHITI_PASSWORD (v1). "
            "Register at https://dahiti.dgfi.tum.de/en/"
        )
        self._api_key = os.environ.get("DAHITI_API_KEY")
        self._username = os.environ.get("DAHITI_USERNAME")
        self._password = os.environ.get("DAHITI_PASSWORD")
        super().__init__()

    # ------------------------------------------------------------------
    # DataAccess interface
    # ------------------------------------------------------------------

    def verify_inputs(self) -> bool:
        self.logger.info("Verifying inputs: " + self.__class__.__name__)
        if not self._has_credentials():
            # Warn but return True — a missing key should not abort the full
            # CLAY run.  product_driver will raise, which CLAY catches per-product.
            self.logger.warning(
                "No DAHITI credentials found. "
                "Set DAHITI_API_KEY (v2) or DAHITI_USERNAME + DAHITI_PASSWORD (v1). "
                "Register at https://dahiti.dgfi.tum.de/en/ — water level data will "
                "be skipped for this run."
            )
        return True

    def attach_geodata(self) -> str:
        # Point observation — no gridded output to attach.
        self.logger.info("Attaching Geo Data inputs: " + self.__class__.__name__)
        return "complete"

    def product_driver(self, polygon, debug=False, run_cleanly=False) -> list[MVSeries]:
        """Fetch DAHITI water level and return as an MVSeries.

        Raises RuntimeError if credentials are missing or the lake has no
        DAHITI coverage.  CLAY catches these per-product and logs a warning.
        """
        self.logger.info("DAHITI Driver Started: Water Level")

        if not self._has_credentials():
            raise RuntimeError(
                "No DAHITI credentials set. "
                "Set DAHITI_API_KEY or DAHITI_USERNAME + DAHITI_PASSWORD. "
                "Register at https://dahiti.dgfi.tum.de/en/"
            )

        if not run_cleanly and not debug:
            try:
                self.logger.info("Attempting to find and read saved DAHITI data")
                series = helpers.unpickle_var("DAHITI")
            except FileNotFoundError:
                self.logger.info("No saved DAHITI data found, calling API")
                series = self._fetch_and_process(polygon)
        else:
            series = self._fetch_and_process(polygon)

        mv = MVSeries(
            dataset=series,
            unit="m",
            single_letter_code="l",
            product_name="DAHITI",
            variable_name="wse",
            xarr_dataset=None,  # point observation, no gridded source
        )

        self._try_fetch_hypsometry(run_cleanly)

        self.send_data_product_event(
            events.DataProductRunEvent("DAHITI", self.README_default_information)
        )
        self.logger.info("DAHITI Driver Finished")
        return [mv]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _has_credentials(self) -> bool:
        return bool(self._api_key) or bool(self._username and self._password)

    def _uses_v2(self) -> bool:
        return bool(self._api_key)

    def find_dahiti_target(self, polygon: Polygon) -> int | None:
        """Return the DAHITI target ID closest to the lake centroid.

        Searches within a padded bounding box.  Returns None when no target
        is within _MAX_DISTANCE_DEG degrees.
        """
        bounds = polygon.bounds  # (min_lon, min_lat, max_lon, max_lat)
        pad = 1.0

        if self._uses_v2():
            params = {
                "api_key": self._api_key,
                "min_lon": bounds[0] - pad,
                "max_lon": bounds[2] + pad,
                "min_lat": bounds[1] - pad,
                "max_lat": bounds[3] + pad,
            }
            resp = requests.get(_API_V2 + "list-targets/", params=params, timeout=30)
        else:
            data = {
                "username": self._username,
                "password": self._password,
                "action": "list-targets",
                "min_lon": bounds[0] - pad,
                "max_lon": bounds[2] + pad,
                "min_lat": bounds[1] - pad,
                "max_lat": bounds[3] + pad,
            }
            resp = requests.post(_API_V1, data=data, timeout=30)

        resp.raise_for_status()
        targets = resp.json()

        if not targets:
            self.logger.warning("No DAHITI targets found within search bounding box")
            return None

        centroid_lat = polygon.centroid.y
        centroid_lon = polygon.centroid.x

        best_id = None
        best_dist = float("inf")
        for t in targets:
            dist = np.hypot(
                float(t["longitude"]) - centroid_lon,
                float(t["latitude"]) - centroid_lat,
            )
            if dist < best_dist:
                best_dist = dist
                # v1 returns "id"; v2 returns "dahiti_id" — handle both
                best_id = int(t.get("dahiti_id") or t.get("id"))

        if best_dist > _MAX_DISTANCE_DEG:
            self.logger.warning(
                "Closest DAHITI target is %.4f degrees away — lake is likely "
                "outside DAHITI coverage",
                best_dist,
            )
            return None

        self.logger.info(
            "Found DAHITI target ID %d (distance=%.4f deg)", best_id, best_dist
        )
        return best_id

    def download_water_level(self, target_id: int) -> pd.Series:
        """Download and monthly-average water level for a DAHITI target ID.

        Returns a pd.Series indexed by month-start timestamps, values in metres.
        """
        if self._uses_v2():
            params = {
                "api_key": self._api_key,
                "dahiti_id": target_id,
                "format": "json",
            }
            resp = requests.get(
                _API_V2 + "download-water-level/", params=params, timeout=60
            )
        else:
            data = {
                "username": self._username,
                "password": self._password,
                "action": "download-water-level",
                "dahiti_id": str(target_id),
            }
            resp = requests.post(_API_V1, data=data, timeout=60)

        resp.raise_for_status()
        raw = resp.json()

        records = [
            {"date": pd.Timestamp(r["date"]), "wse": float(r["wse"])} for r in raw
        ]
        df = pd.DataFrame(records).set_index("date")
        series = df["wse"]
        # Snap all observations to month-start and average within each month.
        series.index = series.index.to_period("M").to_timestamp()
        series = series.groupby(series.index).mean()
        series.name = "wse"
        return series

    def download_hypsometry(self, target_id: int) -> pd.DataFrame:
        """Download area-elevation (hypsometric) relationship from DAHITI.

        Returns a DataFrame sorted ascending by WSE with columns:
            water_surface_elevation_m — WSE in metres (WGS84 ellipsoidal)
            surface_area_km2          — lake surface area in km²

        Raises RuntimeError if the endpoint returns an empty or unparseable response.
        """
        if self._uses_v2():
            params = {"api_key": self._api_key, "dahiti_id": target_id}
            resp = requests.get(
                _API_V2 + "download-hypsometry/", params=params, timeout=60
            )
        else:
            data = {
                "username": self._username,
                "password": self._password,
                "action": "download-hypsometry",
                "dahiti_id": str(target_id),
            }
            resp = requests.post(_API_V1, data=data, timeout=60)

        resp.raise_for_status()
        raw = resp.json()

        if not raw:
            raise RuntimeError(
                "No hypsometry data returned for DAHITI target %d. "
                "Hypsometric curves are not available for all lakes." % target_id
            )

        rows = []
        for r in raw:
            wse = r.get("wse") or r.get("stage") or r.get("elevation")
            area_val = r.get("area") or r.get("surface_area") or r.get("area_km2")
            if wse is None or area_val is None:
                continue
            rows.append(
                {
                    "water_surface_elevation_m": float(wse),
                    "surface_area_km2": float(area_val),
                }
            )

        if not rows:
            raise RuntimeError(
                "Could not parse hypsometry records for DAHITI target %d. "
                "Check API response format." % target_id
            )

        df = pd.DataFrame(rows)
        df = df.sort_values("water_surface_elevation_m").reset_index(drop=True)
        return df

    def _try_fetch_hypsometry(self, run_cleanly: bool = False) -> None:
        """Download and store the hypsometric curve — best-effort, never raises."""
        hyps_path = (
            GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
            + "/"
            + GLHE.CLAY.globals.config["LAKE_NAME"]
            + "_hypsometry.csv"
        )
        if os.path.exists(hyps_path) and not run_cleanly:
            self.logger.info("Using cached hypsometry from %s", hyps_path)
            self._publish_hypsometry_event(hyps_path)
            return
        try:
            target_id = helpers.unpickle_var("DAHITI_target_id")
            hyps_df = self.download_hypsometry(target_id)
            hyps_df.to_csv(hyps_path, index=False)
            self.logger.info("Hypsometric curve written to %s", hyps_path)
            self._publish_hypsometry_event(hyps_path)
        except Exception as e:
            self.logger.warning("Hypsometry not available for this lake: %s", e)

    def _publish_hypsometry_event(self, hyps_path: str) -> None:
        pubsub.EventBus.Publish(
            pubsub.EventBus,
            events.OutputFileEvent(
                GLHE.CLAY.globals.config["LAKE_NAME"] + "_hypsometry.csv",
                hyps_path,
                ".csv",
                "Hypsometric curve: area-elevation relationship from DAHITI",
                events.TypeOfFileLIME.HYPSOMETRY_DATA,
            ),
        )

    def _fetch_and_process(self, polygon: Polygon) -> pd.Series:
        target_id = self.find_dahiti_target(polygon)
        if target_id is None:
            raise RuntimeError(
                "No DAHITI target found within %.1f degrees of lake centroid"
                % _MAX_DISTANCE_DEG
            )
        series = self.download_water_level(target_id)
        helpers.pickle_var(series, "DAHITI")
        helpers.pickle_var(target_id, "DAHITI_target_id")
        return series


if __name__ == "__main__":
    print("This is the DAHITI module, not a script")

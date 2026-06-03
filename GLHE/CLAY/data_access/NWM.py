import fsspec
import geopandas as gpd
import numpy as np
import xarray as xr
from shapely.geometry import Polygon, Point
import GLHE
from GLHE.CALCITE import events, pubsub
from GLHE.CLAY import helpers, xarray_helpers
from GLHE.CLAY.data_access import data_access_parent_class
from GLHE.CLAY.helpers import MVSeries


class NWM(data_access_parent_class.DataAccess):
    xarray_dataset: xr.Dataset
    BUCKET_URL = "s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/lakeout.zarr"
    verification_lat_long = {"lat": 0, "lon": 0}

    def __init__(self):
        self.README_default_information = (
            "Validate this data with the point file labeled 'NWM' in the output folder"
        )
        super().__init__()

    def verify_inputs(self) -> bool:
        """See parent class for description"""
        self.logger.info("Verifying inputs: " + self.__class__.__name__)
        return True

    def attach_geodata(self) -> str:
        self.logger.info("Attaching Geo Data inputs: " + self.__class__.__name__)
        self.logger.warning("Unverified Output")
        lat = self.verification_lat_long["lat"]
        lon = self.verification_lat_long["lon"]

        # Create a GeoDataFrame with a single point feature
        data = {"geometry": [Point(lon, lat)]}
        gdf = gpd.GeoDataFrame(
            data, geometry="geometry", crs=self.xarray_dataset.attrs["proj4"]
        )
        # point_shape_file_dir = os.path.join(GLHE.globals.OUTPUT_DIRECTORY,
        #                                    GLHE.globals.LAKE_NAME + "_NWM_Verification_Point_Shapefile")
        # if not os.path.exists(point_shape_file_dir):
        #    os.mkdir(point_shape_file_dir)
        output_file = os.path.join(
            GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"],
            GLHE.CLAY.globals.config["LAKE_NAME"] + "_NWM_lake_point",
        )
        gdf.to_file(output_file, compression="zip")
        pubsub.EventBus.Publish(
            pubsub.EventBus,
            events.OutputFileEvent(
                output_file,
                output_file,
                ".shp",
                "A shapefile of the center point of the lake that the program chose.",
                events.TypeOfFileLIME.NWM_LAKE_POINT_SHAPEFILENAME,
            ),
        )

        return "complete"

    def find_lake_id(self, polygon: Polygon) -> list[int]:
        """Find the NWM feature_id index closest to the lake centroid using the zarr store coordinates."""
        self.logger.info("Starting Lake ID Searcher")
        centroid_lat = polygon.centroid.y
        centroid_lon = polygon.centroid.x

        ds = xr.open_zarr(fsspec.get_mapper(self.BUCKET_URL, anon=True), consolidated=True)
        lats = ds.latitude.values
        lons = ds.longitude.values

        dists = np.hypot(lons - centroid_lon, lats - centroid_lat)
        feature_id_index = int(dists.argmin())
        closest_lat = float(lats[feature_id_index])
        closest_lon = float(lons[feature_id_index])

        self.logger.info(f"Found Lake ID Verify Correct Placement (Lat, Long): ({closest_lat}, {closest_lon})")
        self.verification_lat_long["lat"] = closest_lat
        self.verification_lat_long["lon"] = closest_lon

        if dists[feature_id_index] > 0.001 and not polygon.contains(Point(closest_lon, closest_lat)):
            self.logger.error("The lake is not in the NWM domain")
            raise Exception("NWM Lake not found, and no alternate NWM source exists")

        self.logger.info("Found Lake ID")
        return [feature_id_index]

    def product_driver(self, polygon, debug=False, run_cleanly=False) -> list[MVSeries]:
        """
        Gets the runoff data for a given polygon

        Parameters
        ----------
        polygon : shapely.geometry.Polygon
            The polygon of the lake
        Returns
        --------
        MVSeries
            The monthly runoff data
        """
        self.logger.info("NWM Driver Started: Inflow & Outflow")
        if not run_cleanly and not debug:
            try:
                self.logger.info("Attempting to find and read saved NWM Land data")
                self.xarray_dataset = helpers.unpickle_var("NWM")
            except FileNotFoundError:
                self.logger.info(
                    "No saved NWM Land data found, calling access functions"
                )
                self.xarray_dataset = self.call_NWM_s3_access_and_process(polygon)
        else:
            self.xarray_dataset = self.call_NWM_s3_access_and_process(polygon)
        self.verification_lat_long["lat"] = self.xarray_dataset.lat.values.item()
        self.verification_lat_long["lon"] = self.xarray_dataset.lon.values.item()
        list_of_MVSeries = xarray_helpers.convert_xarray_dataset_to_mvseries(
            self.xarray_dataset, "inflow", "outflow"
        )
        list_of_MVSeries = helpers.move_date_index_to_first_of_the_month(
            *list_of_MVSeries
        )
        self.send_data_product_event(
            events.DataProductRunEvent("NWM", self.README_default_information)
        )
        self.logger.info("NWM Driver Finished")
        return list_of_MVSeries

    def call_NWM_s3_access_and_process(self, polygon) -> xr.Dataset:
        """Just moving some product driver functions here"""
        list_of_feature_ids = self.find_lake_id(polygon)
        self.xarray_dataset = self.zarr_lakeout_process(list_of_feature_ids)
        self.xarray_dataset = xarray_helpers.label_xarray_dataset_with_product_name(
            self.xarray_dataset, "NWM"
        )
        self.xarray_dataset = xarray_helpers.fix_lat_long_names_in_xarray_dataset(
            self.xarray_dataset
        )
        self.xarray_dataset = xarray_helpers.group_xarray_dataset_by_month(
            self.xarray_dataset
        )
        self.xarray_dataset = xarray_helpers.rename_xarray_units(
            self.xarray_dataset, "m3/s", "inflow", "outflow"
        )
        self.xarray_dataset = xarray_helpers.convert_xarray_dataset_units(
            self.xarray_dataset, "m3/month", "inflow", "outflow"
        )
        helpers.pickle_var(self.xarray_dataset, "NWM")
        return self.xarray_dataset

    def zarr_lakeout_process(self, feature_id_index) -> xr.Dataset:
        """If we are using zarr files, this code can quickly and efficiently give us the"""
        self.logger.info("Accessing NWM Retrospective Data")
        s3_path = self.BUCKET_URL
        ds = xr.open_zarr(fsspec.get_mapper(s3_path, anon=True), consolidated=True)
        dataset = ds.sel(feature_id=ds.feature_id[feature_id_index].item())
        drop = [v for v in ["crs", "water_sfc_elev"] if v in dataset]
        if drop:
            dataset = dataset.drop_vars(drop)

        self.logger.warning(
            "No verification for if this is the correct lake has been implemented yet. Please implement!"
        )
        self.logger.info("Finished loading NWM Retrospective Data")
        return dataset

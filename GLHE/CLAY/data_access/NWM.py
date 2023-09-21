import os

import fsspec
import geopandas as gpd
import xarray as xr
from pubsub import pub
from pyproj import CRS
from shapely.geometry import Polygon, Point
import boto3
import GLHE
from GLHE.CLAY import events, helpers, xarray_helpers
from GLHE.CLAY.data_access import data_access_parent_class
from GLHE.CLAY.helpers import MVSeries


class NWM(data_access_parent_class.DataAccess):
    xarray_dataset: xr.Dataset
    BUCKET_URL = 's3://noaa-nwm-retrospective-2-1-zarr-pds/lakeout.zarr'
    BUCKET_NAME_NETCDF = 'noaa-nwm-retrospective-2-1-pds'
    verification_lat_long = {"lat": 0, "lon": 0}
    s3: boto3.client

    def __init__(self):
        self.s3 = boto3.client("s3", region_name='us-east-1',
                               aws_access_key_id="AKIA6BHGCVJLQKADHHWY",
                               aws_secret_access_key="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")
        self.README_default_information = "Validate this data with the point file labeled 'NWM' in the output folder"
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
        data = {'geometry': [Point(lon, lat)]}
        gdf = gpd.GeoDataFrame(data, geometry='geometry', crs=self.xarray_dataset.attrs['proj4'])
        # point_shape_file_dir = os.path.join(GLHE.globals.OUTPUT_DIRECTORY,
        #                                    GLHE.globals.LAKE_NAME + "_NWM_Verification_Point_Shapefile")
        # if not os.path.exists(point_shape_file_dir):
        #    os.mkdir(point_shape_file_dir)
        output_file = os.path.join(GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"],
                                   GLHE.CLAY.globals.config["LAKE_NAME"] + "_NWM_lake_point")
        gdf.to_file(output_file, compression='zip')
        pub.sendMessage(events.topics["output_file_event"],
                        message=events.OutputFileEvent(output_file, output_file, ".shp",
                                                       "A shapefile of the center point of the lake that the program chose.",
                                                       events.TypeOfFileLIME.NWM_LAKE_POINT_SHAPEFILENAME))

        return "complete"

    def find_lake_id(self, polygon: Polygon) -> list[int]:
        """
        Finds the lake ID for a given polygon

        Parameters
        ----------
        polygon : shapely.geometry.Polygon
            The polygon of the lake

        Returns
        -------
        int,bool
            The lake ID
        """
        self.logger.info("Starting Lake ID Searcher")

        latitude = polygon.centroid.y
        longitude = polygon.centroid.x
        lo_file_name = "LocalData/SAMPLE_NWM_LAKEOUT.nc"
        co_file_name = "LocalData/SAMPLE_NWM_CHRTOUT.nc"
        if not os.path.exists(lo_file_name) or not os.path.exists(co_file_name):
            with open(lo_file_name, 'wb') as f:
                self.s3.download_fileobj(self.BUCKET_NAME_NETCDF, "model_output/1979/197902010100.LAKEOUT_DOMAIN1.comp",
                                         f)
            with open(co_file_name, 'wb') as f:
                self.s3.download_fileobj(self.BUCKET_NAME_NETCDF, "model_output/1979/197902010100.CHRTOUT_DOMAIN1.comp",
                                         f)

        lo_nwm = xr.open_dataset(lo_file_name)
        # Select the data corresponding to a specific value
        closest = None

        crs = CRS("EPSG:4326")
        for i, val in enumerate(lo_nwm.feature_id):
            dist = ((longitude - lo_nwm.longitude[i].values) ** 2 + (
                    latitude - lo_nwm.latitude[i].values) ** 2) ** 0.5
            if closest is None or dist < closest[0]:
                closest = (dist, i, lo_nwm.longitude[i].values, lo_nwm.latitude[i].values)
        feature_id_index = closest[1]
        self.logger.info("Found Lake ID Verify Correct Placement (Lat, Long): (" + str(
            lo_nwm.latitude[feature_id_index].item()) + ", " + str(
            lo_nwm.longitude[feature_id_index].item()) + ")")
        self.verification_lat_long["lat"] = lo_nwm.latitude[feature_id_index].item()
        self.verification_lat_long["lon"] = lo_nwm.longitude[feature_id_index].item()
        if closest[0] > 0.001 and not polygon.contains(Point(longitude, latitude)):
            self.logger.error("The lake is not in the NWM domain")
            raise Exception("NWM Lake not found, and no alternate NWM source exists")
        else:
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
                self.logger.info("No saved NWM Land data found, calling access functions")
                self.xarray_dataset = self.call_NWM_s3_access_and_process(polygon)
        else:
            self.xarray_dataset = self.call_NWM_s3_access_and_process(polygon)
        self.verification_lat_long["lat"] = self.xarray_dataset.lat.values.item()
        self.verification_lat_long["lon"] = self.xarray_dataset.lon.values.item()
        list_of_MVSeries = xarray_helpers.convert_xarray_dataset_to_mvseries(self.xarray_dataset, "inflow", "outflow")
        list_of_MVSeries = helpers.move_date_index_to_first_of_the_month(*list_of_MVSeries)
        self.send_data_product_event(events.DataProductRunEvent("NWM", self.README_default_information))
        self.logger.info("NWM Driver Finished")
        return list_of_MVSeries

    def call_NWM_s3_access_and_process(self, polygon) -> xr.Dataset:
        """Just moving some product driver functions here"""
        list_of_feature_ids = self.find_lake_id(polygon)
        self.xarray_dataset = self.zarr_lakeout_process(list_of_feature_ids)
        self.xarray_dataset = xarray_helpers.label_xarray_dataset_with_product_name(self.xarray_dataset, "NWM")
        self.xarray_dataset = xarray_helpers.fix_lat_long_names_in_xarray_dataset(self.xarray_dataset)
        self.xarray_dataset = xarray_helpers.group_xarray_dataset_by_month(self.xarray_dataset)
        self.xarray_dataset = xarray_helpers.rename_xarray_units(self.xarray_dataset, "m3/s", "inflow", "outflow")
        self.xarray_dataset = xarray_helpers.convert_xarray_dataset_units(self.xarray_dataset, "m3/month", "inflow",
                                                                          "outflow")
        helpers.pickle_var(self.xarray_dataset, "NWM")
        return self.xarray_dataset

    def zarr_lakeout_process(self, feature_id_index) -> xr.Dataset:
        """If we are using zarr files, this code can quickly and efficiently give us the """
        self.logger.info("Accessing NWM Retrospective Data")
        s3_path = self.BUCKET_URL
        ds = xr.open_zarr(fsspec.get_mapper(s3_path, anon=True), consolidated=True)
        dataset = ds.sel(feature_id=ds.feature_id[feature_id_index].item())
        dataset = dataset.drop_vars(["crs", "water_sfc_elev"])

        self.logger.warning(
            "No verification for if this is the correct lake has been implemented yet. Please implement!")
        self.logger.info("Finished loading NWM Retrospective Data")
        return dataset

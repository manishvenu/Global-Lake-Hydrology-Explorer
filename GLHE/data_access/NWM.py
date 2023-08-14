import logging
import os
from datetime import datetime as dt

import boto3
import fsspec
import pandas as pd
import xarray as xr
from shapely.geometry import Polygon, Point
import geopandas as gpd
import GLHE
from GLHE.data_access import data_access_parent_class
from GLHE.helpers import MVSeries
from GLHE import helpers, xarray_helpers, events
from pubsub import pub
from pyproj import CRS, Transformer


class NWM(data_access_parent_class.DataAccess):
    xarray_dataset: xr.Dataset
    BUCKET_NAME = 'noaa-nwm-retrospective-2-1-pds'
    s3: boto3.client
    nwm_bulk_message_logger: logging.Logger
    full_data = {"Date": [], "Inflow": [], "Outflow": [], "units": "m3/s", "name": "NWM"}
    verification_lat_long = {"lat": 0, "lon": 0}

    def __init__(self):
        self.s3 = boto3.client("s3", region_name='us-east-1',
                               aws_access_key_id="AKIA6BHGCVJLQKADHHWY",
                               aws_secret_access_key="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")
        self.nwm_bulk_message_logger = logging.getLogger("NWM_Bulk")
        self.nwm_bulk_message_logger.propagate = False
        fh = logging.FileHandler(
            os.path.join(str(GLHE.globals.LOGGING_DIRECTORY), "NWM_Bulk.log"))
        self.nwm_bulk_message_logger.addHandler(fh)
        self.nwm_bulk_message_logger.info("***Starting NWM Object***")
        self.README_default_information = "Validate this data with the point file labeled 'NWM' in the output folder"
        super().__init__()

    def verify_inputs(self) -> bool:
        """See parent class for description"""
        self.logger.info("Verifying inputs: " + self.__class__.__name__)
        return True

    def attach_geodata(self) -> str:
        self.logger.info("Attaching Geo Data inputs: " + self.__class__.__name__)
        lat = self.verification_lat_long["lat"]
        lon = self.verification_lat_long["lon"]

        # Create a GeoDataFrame with a single point feature
        data = {'geometry': [Point(lon, lat)]}
        gdf = gpd.GeoDataFrame(data, geometry='geometry', crs=self.xarray_dataset.attrs['proj4'])
        point_shape_file_dir = os.path.join(GLHE.globals.OUTPUT_DIRECTORY,
                                            GLHE.globals.LAKE_NAME + "_NWM_Verification_Point_Shapefile")
        if not os.path.exists(point_shape_file_dir):
            os.mkdir(point_shape_file_dir)
        output_file = os.path.join(point_shape_file_dir, GLHE.globals.LAKE_NAME + "_NWM_lake_point" + ".shp")
        gdf.to_file(output_file)
        pub.sendMessage(events.topics["output_file_event"],
                        message=events.OutputFileEvent(output_file, output_file, ".shp",
                                                       "A sahpefile file of the center point of the lake that the program chose."))

        return "complete"

    def find_lake_id(self, polygon: Polygon) -> tuple[list[int], bool]:
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
        flag_stream_links_used = False
        lo_file_name = "LocalData/SAMPLE_NWM_LAKEOUT.nc"
        co_file_name = "LocalData/SAMPLE_NWM_CHRTOUT.nc"
        if not os.path.exists(lo_file_name) or not os.path.exists(co_file_name):
            with open(lo_file_name, 'wb') as f:
                self.s3.download_fileobj(self.BUCKET_NAME, "model_output/1979/197902010100.LAKEOUT_DOMAIN1.comp", f)
            with open(co_file_name, 'wb') as f:
                self.s3.download_fileobj(self.BUCKET_NAME, "model_output/1979/197902010100.CHRTOUT_DOMAIN1.comp", f)

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
            flag_stream_links_used = True
            self.logger.error("The lake is not in the NWM domain")
            self.logger.error("THIS MODULE HAS NOT BEEN IMPLEMENTED. IT'S NOT CORRECT!")
            raise Exception("Code has not been implemented yet")
            feature_ids_index = []
            return feature_ids_index, flag_stream_links_used
        else:
            self.logger.info("Found Lake ID")
            return [feature_id_index], flag_stream_links_used

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
        list_of_MVSeries = xarray_helpers.convert_xarray_dataset_to_mvseries(self.xarray_dataset, "inflow", "outflow")
        list_of_MVSeries = helpers.move_date_index_to_first_of_the_month(*list_of_MVSeries)
        self.send_data_product_event(events.DataProductRunEvent("NWM", self.README_default_information))
        self.logger.info("NWM Driver Finished")
        return list_of_MVSeries

    def call_NWM_s3_access_and_process(self, polygon) -> xr.Dataset:
        """Just moving some product driver functions here"""
        list_of_feature_ids, flag_stream_links_used = self.find_lake_id(polygon)
        if (flag_stream_links_used):
            self.chrtout_file_process(list_of_feature_ids)
        else:
            self.xarray_dataset = self.zarr_lakeout_process(list_of_feature_ids)
        self.xarray_dataset = xarray_helpers.label_xarray_dataset_with_product_name(self.xarray_dataset, "NWM")
        self.xarray_dataset = xarray_helpers.fix_lat_long_names_in_xarray_dataset(self.xarray_dataset)
        self.xarray_dataset = xarray_helpers.group_xarray_dataset_by_month(self.xarray_dataset)
        self.xarray_dataset = xarray_helpers.rename_xarray_units(self.xarray_dataset, "m3/s", "inflow", "outflow")
        self.xarray_dataset = xarray_helpers.convert_xarray_dataset_units(self.xarray_dataset, "m3/month", "inflow",
                                                                          "outflow")
        helpers.pickle_var(self.xarray_dataset, "NWM")
        return self.xarray_dataset

    def lakeout_file_process(self, list_of_feature_ids):
        """If we are using lake files
        Parameters
        ----------
        list_of_feature_ids : list
            List of feature ids
        Returns
        -------
        None
        """

        start_date, end_date = self.check_save_file()
        if start_date is None:
            start_date = dt.strptime("1931-02-01", '%Y-%m-%d')
            end_date = dt.strptime("1900-02-28", '%Y-%m-%d')
        if end_date.date() == dt.strptime("2020-12-31", '%Y-%m-%d').date():
            self.logger.info("NWM data is fully updated")
            return
        # Create Iterator
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.BUCKET_NAME, Prefix='model_output/')
        self.logger.info("Iterating through NWM files")
        for page in pages:
            if int(page['Contents'][-1]['Key'].split("/")[1]) < end_date.year:
                self.nwm_bulk_message_logger.info(
                    "Skipping this page of data...." + str(page['Contents'][-1]['Key'].split("/")[1]))
                continue
            for obj in page['Contents']:

                # Accessing only netcdf files with Lake Information #
                if obj['Key'].endswith("comp") and obj["Key"].split("/")[2].split(".")[1].split("_")[0] == "LAKEOUT":
                    filename = obj["Key"].split("/")[2] + ".nc"
                    date = dt.strptime(filename.split(".")[0], '%Y%m%d%H%M')
                    filepath = ".temp/TEMPORARY_NWM_" + filename

                    ## Access only the file at 1200 time (We don't need hourly files) ##
                    if date.hour != 12:
                        continue
                    if date <= end_date:
                        self.nwm_bulk_message_logger.info("NWM File Skipped: " + str(date))
                        continue
                    if date.day == 1:
                        self.save_interim()
                    # Download File ##
                    with open(filepath, 'wb') as f:
                        self.s3.download_fileobj(self.BUCKET_NAME, obj["Key"], f)
                    dict_data = self.extract_data(filepath, list_of_feature_ids, date)
                    self.full_data["Date"].append(dict_data['date'])
                    self.full_data["Outflow"].append(dict_data['outflow'])
                    self.full_data["Inflow"].append(dict_data['inflow'])
                    self.nwm_bulk_message_logger.info("NWM File Processed: " + str(date))
                    os.remove(filepath)

    def chrtout_file_process(self, list_of_feature_ids):
        """If we are using stream files"""
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.BUCKET_NAME, Prefix='model_output/')
        self.logger.info("Iterating through NWM files")
        full_data = {"Date": [], "Inflow": [], "Outflow": []}
        for page in pages:
            for obj in page['Contents']:
                if obj['Key'].endswith("comp") and obj["Key"].split("/")[2].split(".")[1].split("_")[0] == "CHRTOUT":
                    filename = obj["Key"].split("/")[2] + ".nc"
                    date = dt.strptime(filename.split(".")[0], '%Y%m%d%H%M')
                    co_filepath = ".temp/TEMPORARY_NWM_" + filename
                    with open(co_filepath, 'wb') as f:
                        self.s3.download_fileobj(self.BUCKET_NAME, obj["Key"], f)
                        self.logger.info("Succesfully Downloaded", filename)
                        f.close()

    def extract_data(self, nwm_file: xr.Dataset, feature_id: int, date: dt):

        ## Open File ##
        ds_nwm = xr.open_dataset(nwm_file)

        ## Find the streams we want ##
        ds_nwm_wanted = ds_nwm.sel(feature_id=ds_nwm.feature_id[feature_id].values[0])

        ## Access inflow values ##
        inflow = ds_nwm_wanted['inflow'].item()
        outflow = ds_nwm_wanted['outflow'].item()
        ## Add streamflows to output by ID ##
        data_aa = {'inflow': inflow, "date": date, "outflow": outflow}

        ## Return data ##
        return data_aa

    def save_interim(self) -> None:
        """Saves the interim data"""
        filepath = GLHE.globals.OUTPUT_DIRECTORY + "/save_files/" + GLHE.globals.LAKE_NAME + "_" + "NWM.csv"
        df = pd.DataFrame(self.full_data)
        df.to_csv(filepath)

    def check_save_file(self) -> tuple[str]:
        """Checks if the file is already saved and returns the first and last date of the file"""
        filepath = GLHE.globals.OUTPUT_DIRECTORY + "/save_files/" + GLHE.globals.LAKE_NAME + "_" + "NWM.csv"
        if os.path.exists(filepath):
            saved_NWM_data = pd.read_csv(filepath, skipinitialspace=True)
            self.full_data["Date"] = pd.to_datetime(saved_NWM_data["Date"]).tolist()
            self.full_data["Inflow"] = saved_NWM_data["Inflow"].tolist()
            self.full_data["Outflow"] = saved_NWM_data["Outflow"].tolist()
            if len(self.full_data["Date"]) < 2:
                return None, None
            return self.full_data["Date"][0], self.full_data["Date"][-1]
        else:
            return None, None

    def zarr_lakeout_process(self, feature_id_index) -> xr.Dataset:
        """If we are using zarr files, this code can quickly and efficiently give us the """
        self.logger.info("Accessing NWM Retrospective Data")
        s3_path = 's3://noaa-nwm-retrospective-2-1-zarr-pds/lakeout.zarr'
        ds = xr.open_zarr(fsspec.get_mapper(s3_path, anon=True), consolidated=True)
        dataset = ds.sel(feature_id=ds.feature_id[feature_id_index].item())
        dataset = dataset.drop_vars(["crs", "water_sfc_elev"])

        self.logger.warning(
            "No verification for if this is the correct lake has been implemented yet. Please implement!")
        self.logger.info("Finished loading NWM Retrospective Data")
        return dataset

import logging
import os
from datetime import datetime as dt

import boto3
import pandas as pd
import xarray as xr
from shapely.geometry import Polygon, Point

import GLHE
from GLHE import helpers
from GLHE.data_access import data_access_parent_class
from GLHE.helpers import MVSeries


class NWM(data_access_parent_class.DataAccess):
    xarray_dataset: xr.Dataset
    BUCKET_NAME = 'noaa-nwm-retrospective-2-1-pds'
    s3: boto3.client
    nwm_bulk_message_logger: logging.Logger
    full_data = {"Date": [], "Inflow": [], "Outflow": [], "units": "m3/s", "name": "NWM"}

    def __init__(self):
        self.s3 = boto3.client("s3", region_name='us-east-1',
                               aws_access_key_id="AKIA6BHGCVJLQKADHHWY",
                               aws_secret_access_key="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")
        self.nwm_bulk_message_logger = logging.getLogger("NWM_Bulk")
        fh = logging.FileHandler(
            'C:\\Users\\manis\\OneDrive - Umich\\Documents\\Global Lake Hydrology Explorer\\GLHE\\.temp\\GLHE_NWM_Bulk.log')
        self.nwm_bulk_message_logger.addHandler(fh)
        # self.nwm_bulk_message_logger.info("***Starting NWM Object***")

        super().__init__()

    def verify_inputs(self) -> bool:
        """See parent class for description"""
        self.logger.info("Verifying inputs: " + self.__class__.__name__)
        return True

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
        lo_file_name = ".temp/TEMPORARY_NWM_LAKEOUT.nc"
        co_file_name = ".temp/TEMPORARY_NWM_CHRTOUT.nc"
        if not os.path.exists(lo_file_name) or not os.path.exists(co_file_name):
            with open(lo_file_name, 'wb') as f:
                self.s3.download_fileobj(self.BUCKET_NAME, "model_output/1979/197902010100.LAKEOUT_DOMAIN1.comp", f)
            with open(co_file_name, 'wb') as f:
                self.s3.download_fileobj(self.BUCKET_NAME, "model_output/1979/197902010100.CHRTOUT_DOMAIN1.comp", f)

        lo_nwm = xr.open_dataset(lo_file_name)
        # Select the data corresponding to a specific value
        closest = None
        for i, val in enumerate(lo_nwm.feature_id):
            dist = ((longitude - lo_nwm.longitude[i].values) ** 2 + (
                    latitude - lo_nwm.latitude[i].values) ** 2) ** 0.5
            if closest is None or dist < closest[0]:
                closest = (dist, i, lo_nwm.longitude[i].values, lo_nwm.latitude[i].values)
        feature_id_index = closest[1]
        self.logger.info("Found Lake ID Verify Correct Placement (Lat, Long): (" + str(
            lo_nwm.latitude[feature_id_index].item()) + ", " + str(
            lo_nwm.longitude[feature_id_index].item()) + ")")
        if closest[0] > 0.001 and not polygon.contains(Point(longitude, latitude)):
            flag_stream_links_used = True
            self.logger.error("The lake is not in the NWM domain")
            self.logger.error("THIS MODULE HAS NOT BEEN IMPLEMENTED. IT'S NOT CORRECT!")
            # co_nwm = xr.open_dataset(co_file_name)
            raise Exception("Code has not been implemented yet")
            feature_ids_index = []
            # for count, i in enumerate(co_nwm.feature_id):
            #     print(count)
            #     point = Point(co_nwm.longitude[count].values, co_nwm.latitude[count].values)
            #     if point.within(polygon):
            #         feature_ids_index.append(count)
            return feature_ids_index, flag_stream_links_used
        else:
            self.logger.info("Found Lake ID")
            return [feature_id_index], flag_stream_links_used

    def product_driver(self, polygon, debug=False) -> list[MVSeries]:
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

        list_of_feature_ids, flag_stream_links_used = self.find_lake_id(polygon)
        if (flag_stream_links_used):
            self.chrtout_file_process(list_of_feature_ids)
        else:
            self.lakeout_file_process(list_of_feature_ids)
            self.save_interim()
        list_of_MVSeries_no_units = helpers.convert_dicts_to_MVSeries("Date", "units", "name", self.full_data)
        list_of_MVSeries_grouped_to_month = helpers.group_MVSeries_by_month(list_of_MVSeries_no_units)
        list_of_MVSeries_converted_units = helpers.convert_MVSeries_units(list_of_MVSeries_grouped_to_month, "m3/month")
        self.logger.info("NWM Driver Finished")
        return list_of_MVSeries_converted_units

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

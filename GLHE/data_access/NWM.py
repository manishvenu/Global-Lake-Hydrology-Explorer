from datetime import datetime as dt

import boto3
import xarray as xr
from shapely.geometry import Polygon, Point

from GLHE.data_access import data_access_parent_class
from GLHE.helpers import MVSeries


class NWM(data_access_parent_class.DataAccess):
    xarray_dataset: xr.Dataset
    BUCKET_NAME = 'noaa-nwm-retrospective-2-1-pds'
    s3: boto3.client

    def __init__(self):
        s3 = boto3.client("s3", region_name='us-east-1',
                          aws_access_key_id="AKIA6BHGCVJLQKADHHWY",
                          aws_secret_access_key="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")
        super().__init__()

    def verify_inputs(self) -> bool:
        """See parent class for description"""
        return True

    def product_driver(self, polygon, debug=False) -> list[MVSeries]:
        """See parent class for description, unimplemnted"""
        raise NotImplementedError()

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

        # Define the latitude and longitude coordinates
        latitude = polygon.centroid.y
        longitude = polygon.centroid.x

        flag_stream_links_used = False
        # Use Iterator to find a lakeout file
        lo_file_name = ".temp/TEMPORARY_NWM_LAKEOUT.nc"
        co_file_name = ".temp/TEMPORARY_NWM_CHRTOUT.nc"
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
        if closest[0] > 0.0001:
            flag_stream_links_used = True
            self.logger.error("The lake is not in the NWM domain")
            self.logger.error(
                "Doing it the hard way, accessing direct stream links in the area, if this lake is incorrectly "
                "placed in the HydroLakes database, this data is bogus, flags will be added here, and a map downloaded. check "
                "it!")
            co_nwm = xr.open_dataset(co_file_name)
            feature_ids_index = []
            for count, i in enumerate(co_nwm.feature_id):
                print(count)
                point = Point(co_nwm.longitude[count].values, co_nwm.latitude[count].values)
                if point.within(polygon):
                    feature_ids_index.append(count)
            return feature_ids_index, flag_stream_links_used
        else:
            return [feature_id_index], flag_stream_links_used

    def get_nwm_runoff(self, polygon: Polygon) -> MVSeries:
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
        list_of_feature_ids, flag_stream_links_used = self.find_lake_id(polygon)
        # Create Iterator
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.BUCKET_NAME, Prefix='model_output/')
        for page in pages:
            for obj in page['Contents']:
                # Accessing only netcdf files with Lake Information ##
                if (not flag_stream_links_used) and obj['Key'].endswith("comp") and \
                        obj["Key"].split("/")[2].split(".")[1].split("_")[0] == "LAKEOUT":
                    filename = obj["Key"].split("/")[2] + ".nc"
                    date = dt.strptime(filename.split(".")[0], '%Y%m%d%H%M')
                    filepath = ".temp/TEMPORARY_" + filename
                    # Download File ##
                    with open(filepath, 'wb') as f:
                        self.s3.download_fileobj(self.BUCKET_NAME, obj["Key"], f)
                        self.logger.info("Succesfully Downloaded", filename)
                        f.close()

                    file_found = True
                elif flag_stream_links_used and obj['Key'].endswith("comp") and \
                        obj["Key"].split("/")[2].split(".")[1].split("_")[0] == "CHRTOUT":
                    filename = obj["Key"].split("/")[2] + ".nc"
                    date = dt.strptime(filename.split(".")[0], '%Y%m%d%H%M')
                    co_filepath = ".temp/TEMPORARY_" + filename
                    # Download File ##
                    with open(co_filepath, 'wb') as f:
                        self.s3.download_fileobj(self.BUCKET_NAME, obj["Key"], f)
                        self.logger.info("Succesfully Downloaded", filename)
                        f.close()

        return None

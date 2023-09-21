import os
import zipfile
import json
import holoviews as hv
from raster2xyz.raster2xyz import Raster2xyz
import pandas as pd
from pathlib import Path


def check_or_create_directory(dir: str) -> None:
    if not os.path.exists(dir):
        os.mkdir(dir)
    return


class GriddedDataDisplay:
    config: dict
    tif_file_paths = {}
    dest_dir = os.path.join(".temp", "tifs")

    def __init__(self, config: dict):
        """Initializes the data access class"""
        check_or_create_directory(".temp")
        check_or_create_directory(".temp/tifs")
        with zipfile.ZipFile(os.path.join(config["GRIDDED_DATA_FOLDER"]),
                             'r') as zip_ref:
            for filename in zip_ref.namelist():
                split_list = filename.split("_")
                metadata = {"var": split_list[0], "product": split_list[1], "date": split_list[-1].split(".")[0]}

                file_path = os.path.join(self.dest_dir)
                zip_ref.extract(filename, file_path)
                self.tif_file_paths[metadata["var"] + "." + metadata["product"]] = os.path.join(file_path, filename)

    def get_points(self, key: str) -> hv.Points:
        input_raster = self.tif_file_paths[key]
        out_csv = os.path.join(self.dest_dir, self.tif_file_paths[key].split("\\")[-1].split(".")[0] + ".csv")
        if not os.path.exists(out_csv):
            rtxyz = Raster2xyz()
            rtxyz.translate(input_raster, out_csv)
        grid_df = pd.read_csv(out_csv)
        grid_df["easting"], grid_df["northing"] = hv.Tiles.lon_lat_to_easting_northing(
            grid_df["x"], grid_df["y"]
        )
        points = hv.Points(grid_df, ["easting", "northing"], 'z')
        return points

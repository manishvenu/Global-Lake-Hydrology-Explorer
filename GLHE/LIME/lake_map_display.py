import numpy as np
import geopandas as gpd
import holoviews as hv


class LakeMapDisplay:
    """Shows the NWM lake validation point on an interactive tile map."""

    def __init__(self, config: dict):
        self.config = config
        self.has_nwm = "NWM_LAKE_POINT_SHAPEFILENAME" in config

    def make_map(self) -> hv.Overlay:
        tiles = hv.element.tiles.CartoLight()
        if not self.has_nwm:
            return tiles.opts(
                width=700, height=450, title="Lake Location (no NWM point available)"
            )
        gdf = gpd.read_file(self.config["NWM_LAKE_POINT_SHAPEFILENAME"])
        lons = gdf.geometry.x.values
        lats = gdf.geometry.y.values
        eastings, northings = hv.Tiles.lon_lat_to_easting_northing(lons, lats)
        points = hv.Points(
            {"easting": eastings, "northing": northings},
            kdims=["easting", "northing"],
        ).opts(
            size=12,
            color="red",
            marker="triangle",
            tools=["hover"],
            width=700,
            height=450,
            title="NWM Lake Validation Point",
        )
        return tiles * points

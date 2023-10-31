import geopandas as gpd
import os
import plotly.express as px
import json
from dash import dcc
from pathlib import Path


class LakePointDisplay:
    nwm_point_df: gpd.GeoDataFrame
    center_point = (0, 0)
    no_data = False

    def __init__(self, config: dict):
        if "NWM_LAKE_POINT_SHAPEFILENAME" not in config:
            self.no_data = True
            print("No NWM Lake Point Shapefile found. Skipping NWM Lake Point Display.")
            return
        self.nwm_point_df = gpd.read_file(
            os.path.join(config["NWM_LAKE_POINT_SHAPEFILENAME"]))
        self.nwm_point_df = self.nwm_point_df.rename(columns={'time': 'Date'})
        self.center_point = [(self.nwm_point_df.geometry.y[0], self.nwm_point_df.geometry.x[0])]

    def generate_nwm_point_figure(self) -> dcc.Graph:
        """
        Given a config file, the function returns a DASH Data table.
        """
        if self.no_data:
            return dcc.Graph(id="nwm_point_graph", figure=dict(data=[], layout=dict()))
        fig = px.scatter_mapbox(self.nwm_point_df, lat=self.nwm_point_df.geometry.y,
                                lon=self.nwm_point_df.geometry.x,
                                zoom=7, title="NWM Lake Validation", mapbox_style='open-street-map')
        graph = dcc.Graph(id="nwm_point_graph", figure=fig)
        return graph

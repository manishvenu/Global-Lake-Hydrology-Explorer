import geopandas as gpd
import os
import plotly.express as px
import json
from dash import dcc
from pathlib import Path


class LakePointDisplay:
    nwm_point_df: gpd.GeoDataFrame

    def __init__(self):
        """Initializes the data access class"""
        with open(os.path.join(Path(__file__).parent, 'config', 'config.json'), 'r') as f:
            config = json.load(f)
        self.nwm_point_df = gpd.read_file(
            os.path.join(config["CLAY_OUTPUT_FOLDER_LOCATION"], config["NWM_LAKE_POINT_SHAPEFILENAME"]))
        self.nwm_point_df = self.nwm_point_df.rename(columns={'time': 'Date'})

    def generate_nwm_point_figure(self) -> dcc.Graph:
        """
        Given a config file, the function returns a DASH Data table.
        """
        fig = px.scatter_mapbox(self.nwm_point_df, lat=self.nwm_point_df.geometry.y,
                                lon=self.nwm_point_df.geometry.x,
                                zoom=7, title="NWM Lake Validation", mapbox_style='open-street-map')
        graph = dcc.Graph(id="nwm_point_graph", figure=fig)
        return graph

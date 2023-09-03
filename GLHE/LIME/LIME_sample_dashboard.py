import os
import zipfile

import dash_bootstrap_components as dbc
import geopandas as gpd
import holoviews as hv
from holoviews import opts
import pandas as pd
import plotly.express as px
from dash import Dash, html, dash_table, dcc, Input, Output
from holoviews.plotting.plotly.dash import to_dash
from raster2xyz.raster2xyz import Raster2xyz


def check_or_create_directory(dir: str) -> None:
    if not os.path.exists(dir):
        os.mkdir(dir)
    return


df = pd.read_csv(
    os.path.join(os.path.normpath(os.getcwd() + os.sep + os.pardir + os.sep + os.pardir), "LakeOutputDirectory",
                 "Great_Salt",
                 "Great_Salt_Data.csv"))
nwm_point_df = gpd.read_file(
    os.path.join(os.path.normpath(os.getcwd() + os.sep + os.pardir + os.sep + os.pardir),
                 "LakeOutputDirectory",
                 "Great_Salt",
                 "Great_Salt_NWM_lake_point", "Great_Salt_NWM_lake_point.shp"))
fig = px.scatter_mapbox(nwm_point_df, lat=nwm_point_df.geometry.y,
                        lon=nwm_point_df.geometry.x,
                        zoom=7, title="NWM Lake Validation", mapbox_style='open-street-map')
df = df.rename(columns={'time': 'Date'})

tif_file_paths = []
check_or_create_directory(".temp")
check_or_create_directory(".temp/tifs")
with zipfile.ZipFile(os.path.join(os.path.normpath(os.getcwd() + os.sep + os.pardir + os.sep + os.pardir),
                                  "LakeOutputDirectory",
                                  "Great_Salt",
                                  "Great_Salt_gridded_data_layers_on_20020501.zip"), 'r') as zip_ref:
    for filename in zip_ref.namelist():
        split_list = filename.split("_")
        metadata = {"var": split_list[0], "product": split_list[1], "date": split_list[-1].split(".")[0]}

        dest_dir = os.path.join(".temp", "tifs")
        file_path = os.path.join(dest_dir)
        zip_ref.extract(filename, file_path)
        tif_file_paths.append(os.path.join(file_path, filename))

# 1 Sample Set
input_raster = tif_file_paths[0]
out_csv = os.path.join(dest_dir, tif_file_paths[0].split("\\")[-1].split(".")[0] + ".csv")
rtxyz = Raster2xyz()
rtxyz.translate(input_raster, out_csv)
grid_df = pd.read_csv(out_csv)
grid_df["easting"], grid_df["northing"] = hv.Tiles.lon_lat_to_easting_northing(
    grid_df["x"], grid_df["y"]
)
points = hv.Points(grid_df, ["easting", "northing"])
tiles = hv.element.tiles.CartoLight()
overlay = tiles * points
overlay.opts(opts.Overlay(title='Gridded Data Validation'))
app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY], )

components = to_dash(app, [overlay])
app.layout = html.Div([
    dbc.Row(dbc.Col(html.H1('Great Salt Lake, Utah, USA', style={'textAlign': 'center'}))),
    dbc.Row(dbc.Col(dcc.Graph(id="precip_graph"))),
    dbc.Row(dbc.Col(
        dcc.Checklist(
            id="precip_checklist",
            options=[
                {'label': 'CRUTS Precipitation', 'value': 'p.CRUTS'},
                {'label': 'ERA5 Land Precipitation', 'value': 'p.ERA5_Land'},
            ],
            value=['p.CRUTS'],
            inline=True
        ))),
    dbc.Row(children=[dbc.Col(
        dash_table.DataTable(data=df.to_dict('records'), page_size=10)),
        dbc.Col(components.children),
        dbc.Col(dcc.Graph(id="nwm_point_graph", figure=fig))
    ])
])


@app.callback(
    Output("precip_graph", "figure"),
    Input("precip_checklist", "value"))
def update_line_chart(plot_columns):
    y_cols = plot_columns
    fig = px.line(df, x="Date", y=y_cols,
                  labels={
                      "Date": "Date",
                      "value": "Precipitation (mm/month)",
                      "variable": "Dataset"
                  }, )
    return fig


if __name__ == '__main__':
    app.run(debug=True)

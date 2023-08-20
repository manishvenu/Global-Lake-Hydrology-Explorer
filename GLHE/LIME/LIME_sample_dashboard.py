import os
import zipfile
import rioxarray as rxr
import dash_bootstrap_components as dbc
import geopandas as gpd
import holoviews as hv
import pandas as pd
import plotly.express as px
from dash import Dash, html, dash_table, dcc, Input, Output
from holoviews.plotting.plotly.dash import to_dash
from raster2xyz.raster2xyz import Raster2xyz
import dash_leaflet as dl

hv.extension('bokeh')
color_domain = dict(domainMin=0, domainMax=160183,
                    classes=[0, 5000, 100000, 160183],
                    colorscale=['red', 'green', 'blue'])
px.set_mapbox_access_token(
    "pk.eyJ1IjoibWFuaXNodmVudSIsImEiOiJjbGxkcmptbncwYzR1M2ZyeW1vY3dheWpkIn0.8OQCrEQMTfqFlVvoDmQI-Q")


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
image_url = "/" + tif_file_paths[0]
image_bounds = [[], []]
bounds_tiff = [34.113362591, 4.379137166, 48.020394545, 20.420888158]
dataarray = rxr.open_rasterio(tif_file_paths[0]).rio.reproject('EPSG:3857')
grid_df_2 = dataarray[0].to_pandas()
colormap_to_use = 'terrain'
colormap_to_use = ['#000000', '#444444', '#888888', '#CCCCCC', '#FFFFFF']
# Some arbitrary sizes we will use to display images.
image_height, image_width = 600, 600

# Maps will have the same height, but they will be wider
map_height, map_width = image_height, 1000

# As we've seen, the coordinates in our dataset were called x and y, so we are
# going to use these.
key_dimensions = ['x', 'y']

# We are also going to need the name of the value stored in the file. We get it
# from there this time, but we could also set this manually.
value_dimension = dataarray.attrs["AREA_OR_POINT"]
clipping = {'NaN': '#00000000'}
hv.opts.defaults(
    hv.opts.Image(cmap=colormap_to_use,
                  height=image_height, width=image_width,
                  colorbar=True,
                  tools=['hover'], active_tools=['wheel_zoom'],
                  clipping_colors=clipping),
    hv.opts.Tiles(active_tools=['wheel_zoom'], height=map_height, width=map_width)
)
hv_dataset = hv.Dataset(dataarray[0], vdims=value_dimension, kdims=key_dimensions)
esri_img = hv.element.tiles.EsriImagery().opts(width=600, height=550)
hv_image_basic = hv.Image(hv_dataset).opts(title='Test Grid')
hv_combined_basic = esri_img * hv_image_basic

out_csv = os.path.join(dest_dir, tif_file_paths[0].split("\\")[-1].split(".")[0] + ".csv")
rtxyz = Raster2xyz()
rtxyz.translate(input_raster, out_csv)
grid_df = pd.read_csv(out_csv)
grid_df["easting"], grid_df["northing"] = hv.Tiles.lon_lat_to_easting_northing(
    grid_df["x"], grid_df["y"]
)
points = hv.Points(grid_df, ["easting", "northing"]).opts(color="crimson")
tiles = hv.element.tiles.CartoLight()
overlay = tiles * points
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
        dbc.Col(dcc.Graph(id="nwm_point_graph", figure=px.scatter_mapbox(nwm_point_df, lat=nwm_point_df.geometry.y,
                                                                         lon=nwm_point_df.geometry.x,
                                                                         zoom=5, title="NWM Point Verification")))
    ]),
    dbc.Row(
        children=[
            dbc.Col(dl.Map([dl.GeoTIFFOverlay(id='GEOTIFF_ID', interactive=True, url=tif_file_paths[0], band=0,
                                              opacity=0.9, **color_domain)],
                           style={'width': '100%', 'height': '50vh', 'margin': "auto", "display": "block"}))])
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

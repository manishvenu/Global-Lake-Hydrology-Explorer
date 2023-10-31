import dash_bootstrap_components as dbc
import holoviews as hv
import plotly.express as px
from dash import Dash, html, dcc, Input, Output, State

from flask import Flask
from holoviews import opts
from holoviews.plotting.plotly.dash import to_dash
import os
from pathlib import Path
import json
import GLHE.LIME.gridded_data_validation as gridded_data_validation
import GLHE.LIME.lake_point_validation as lake_point_validation
import GLHE.LIME.series_data_display as series_data_display
import warnings
import reverse_geocode
import threading

warnings.filterwarnings("ignore", category=FutureWarning)

app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY], )


def prep_work(CDO):
    global CLAY_driver_obj
    CLAY_driver_obj = CDO
    with open(os.path.join(Path(__file__).parent, 'config', 'config.json'), 'r') as f:
        config_pointer = json.load(f)
    with open(os.path.join(config_pointer["CLAY_OUTPUT_FOLDER_LOCATION"], 'config.json'), 'r') as f:
        config = json.load(f)
    with open(config["BLEEPBLEEP"], 'r') as f:
        data_products = json.load(f)
    Series_Data_Obj = series_data_display.SeriesDataDisplay(config)
    series_data_table = Series_Data_Obj.generate_series_data_table()
    global series_data_df
    series_data_df = Series_Data_Obj.get_df()
    LakePointDisplay_Obj = lake_point_validation.LakePointDisplay(config)
    NWM_fig = LakePointDisplay_Obj.generate_nwm_point_figure()
    tiles = hv.element.tiles.CartoLight()
    Gridded_Data_Obj = gridded_data_validation.GriddedDataDisplay(config)
    if not LakePointDisplay_Obj.no_data:
        address = reverse_geocode.search(LakePointDisplay_Obj.center_point)
    else:
        address = [{"country": "Unknown Country"}]
    points = Gridded_Data_Obj.get_points("p.CRUTS")
    overlay = tiles * points

    try:
        points2 = Gridded_Data_Obj.get_points("p.ERA5-Land")
        overlay *= points2
    except Exception as e:
        print("No ERA5 Found. Exception:", e)
    overlay.opts(title='Gridded Data Validation')
    components = to_dash(app, [overlay])
    options_list = []
    for key in data_products.keys():
        if not data_products[key]["loaded"]:
            options_list.append(key)
    app.layout = html.Div([
        dbc.Row(dbc.Col(html.H1("Lake " + config["LAKE_NAME"].replace("_", " ") + ", " + address[0]["country"],
                                style={'textAlign': 'center'}))),
        dbc.Row([
            dbc.Col(dcc.Graph(id="p_graph"), width=11),
            dbc.Col(Series_Data_Obj.generate_graph_checklist_by_component("p"), width=1, align="center")]),
        dbc.Row([dbc.Col(dcc.Graph(id="e_graph"), width=11),
                 dbc.Col(Series_Data_Obj.generate_graph_checklist_by_component("e"), width=1, align="center")]),
        dbc.Row([dbc.Col(dcc.Graph(id="i_graph"), width=11),
                 dbc.Col(Series_Data_Obj.generate_graph_checklist_by_component("i"), width=1, align="center")]),
        dbc.Row([dbc.Col(dcc.Graph(id="o_graph"), width=11),
                 dbc.Col(Series_Data_Obj.generate_graph_checklist_by_component("o"), width=1, align="center")]),
        dbc.Row([dbc.Col(series_data_table)]),
        dbc.Row([dbc.Col(components.children), dbc.Col(NWM_fig)]),
        dbc.Row(
            dbc.Col([html.P("Some Products are Slow! Load them here:"), dcc.Dropdown(options_list, id='demo-dropdown'),
                     html.Button('Run Product', id='button'),
                     html.Div(id='dd-output-container')]
                    )),
    ])


@app.callback(
    Output("p_graph", "figure"),
    Input("p_checklist", "value"))
def update_line_chart(plot_columns):
    y_cols = plot_columns
    fig = px.line(series_data_df, x="Date", y=y_cols,
                  labels={
                      "Date": "Date",
                      "value": "Precipitation (mm/month)",
                      "variable": "Dataset"
                  }, )
    return fig


@app.callback(
    Output("e_graph", "figure"),
    Input("e_checklist", "value"))
def update_line_chart(plot_columns):
    y_cols = plot_columns
    fig = px.line(series_data_df, x="Date", y=y_cols,
                  labels={
                      "Date": "Date",
                      "value": "Evaporation (mm/month)",
                      "variable": "Dataset"
                  }, )
    return fig


@app.callback(
    Output("i_graph", "figure"),
    Input("i_checklist", "value"))
def update_line_chart(plot_columns):
    y_cols = plot_columns
    fig = px.line(series_data_df, x="Date", y=y_cols,
                  labels={
                      "Date": "Date",
                      "value": "Inflow (m^3/month)",
                      "variable": "Dataset"
                  }, )
    return fig


@app.callback(
    Output("o_graph", "figure"),
    Input("o_checklist", "value"))
def update_line_chart(plot_columns):
    y_cols = plot_columns
    fig = px.line(series_data_df, x="Date", y=y_cols,
                  labels={
                      "Date": "Date",
                      "value": "Outflow (m^3/month)",
                      "variable": "Dataset"
                  }, )
    return fig


@app.callback(
    Output('dd-output-container', 'children'),
    [Input('button', 'n_clicks')],
    [State(component_id='demo-dropdown', component_property='value')]
)
def update_output(n_clicks, value):
    if n_clicks is not None:
        if n_clicks > 0:
            CLAY_driver_obj.run_product(value)
            prep_work(CLAY_driver_obj)
            return f'You have loaded {value}. Reload the Page!'


if __name__ == '__main__':
    import GLHE.CLAY.CLAY_driver as CLAY_driver

    CLAY_driver_obj = CLAY_driver.CLAY_driver()
    CLAY_driver_obj.main(67)
    prep_work(CLAY_driver_obj)
    app.run()

import dash_bootstrap_components as dbc
import holoviews as hv
import plotly.express as px
from dash import Dash, html, dcc, Input, Output
from holoviews import opts
from holoviews.plotting.plotly.dash import to_dash

import GLHE.LIME.gridded_data_validation as gridded_data_validation
import GLHE.LIME.lake_point_validation as lake_point_validation
import GLHE.LIME.series_data_display as series_data_display
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

Series_Data_Obj = series_data_display.SeriesDataDisplay()
series_data_table = Series_Data_Obj.generate_series_data_table()
series_data_df = Series_Data_Obj.get_df()
LakePointDisplay_Obj = lake_point_validation.LakePointDisplay()
NWM_fig = LakePointDisplay_Obj.generate_nwm_point_figure()
tiles = hv.element.tiles.CartoLight()
Gridded_Data_Obj = gridded_data_validation.GriddedDataDisplay()
points = Gridded_Data_Obj.get_points("p.CRUTS")
points2 = Gridded_Data_Obj.get_points("p.ERA5-Land")

overlay = tiles * points * points2
overlay.opts(title='Gridded Data Validation')
app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY], )

components = to_dash(app, [overlay])
app.layout = html.Div([
    dbc.Row(dbc.Col(html.H1('Great Salt Lake, Utah, USA', style={'textAlign': 'center'}))),
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
    dbc.Row([dbc.Col(components.children), dbc.Col(NWM_fig)])
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


if __name__ == '__main__':
    app.run(debug=True)

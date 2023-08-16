import os
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from dash import Dash, html, dash_table, dcc, Input, Output

df = pd.read_csv(
    os.path.join(os.path.normpath(os.getcwd() + os.sep + os.pardir + os.sep + os.pardir), "LakeOutputDirectory",
                 "Great_Salt",
                 "Great_Salt_Data.csv"))

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    html.H1('Great Salt Lake, Utah, USA', style={'textAlign': 'center'}),
    dcc.Graph(id="precip_graph"),
    dcc.Checklist(
        id="precip_checklist",
        options=[
            {'label': 'CRUTS Precipitation', 'value': 'p.CRUTS'},
            {'label': 'ERA5 Land Precipitation', 'value': 'p.ERA5_Land'},
        ],
        value=['p.CRUTS'],
        inline=True
    ),
    dash_table.DataTable(data=df.to_dict('records'), page_size=10)
])


@app.callback(
    Output("precip_graph", "figure"),
    Input("precip_checklist", "value"))
def update_line_chart(plot_columns):
    y_cols = plot_columns
    fig = px.line(df, x="time", y=y_cols,
                  labels={
                      "time": "Date",
                      "value": "Precipitation (mm/month)",
                      "variable": "Dataset"
                  }, )
    return fig


if __name__ == '__main__':
    app.run(debug=True)

import pandas as pd
import os
import json
from dash import dash_table, dcc
import dash_bootstrap_components as dbc
from pathlib import Path


class SeriesDataDisplay:
    df: pd.DataFrame

    def __init__(self, config: dict):

        self.df = pd.read_csv(config["SERIES_DATA"])
        self.df = self.df.rename(columns={'time': 'Date'})

    def generate_series_data_table(self) -> dash_table.DataTable:
        """
        Given a config file, the function returns a DASH Data table.
        """
        table = dash_table.DataTable(data=self.df.to_dict('records'), page_size=10)
        return table

    def get_df(self) -> pd.DataFrame:
        """
        Simple getter for the df
        """
        return self.df

    def generate_graph_checklist_by_component(self, component: str) -> dcc.Checklist:
        """
        Return Checklist best  on p,e,i,o
        """
        if component[0] not in ['p', 'e', 'i', 'o']:
            raise ValueError("Invalid component")

        temp = [col for col in self.df if col.startswith(component[0])]
        options_list = []
        for col in temp:
            options_list.append({'label': str.split(col, ".")[1], 'value': col})

        checklist = dbc.Checklist(
            id=component + "_checklist",
            options=options_list,
            value=[temp[0]]
        )
        return checklist

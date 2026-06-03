import pandas as pd
import hvplot.pandas  # noqa: F401 — registers .hvplot accessor on DataFrames
import panel as pn

SLC_LABELS = {
    "p": "Precipitation (mm/month)",
    "e": "Evapotranspiration (mm/month)",
    "i": "Inflow (m³/month)",
    "o": "Outflow (m³/month)",
}


class SeriesDataDisplay:
    def __init__(self, csv_path: str):
        self.df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        self.df.index.name = "Date"

    def make_plots(self) -> list:
        """One hvplot line chart per variable type (p/e/i/o) that has data."""
        plots = []
        for slc, label in SLC_LABELS.items():
            cols = [c for c in self.df.columns if c.startswith(slc + ".")]
            if cols:
                plot = self.df[cols].hvplot.line(
                    ylabel=label,
                    responsive=True,
                    height=280,
                    legend="top_left",
                    grid=True,
                )
                plots.append(plot)
        return plots

    def make_table(self) -> pn.widgets.Tabulator:
        """Scrollable data table with export support."""
        return pn.widgets.Tabulator(
            self.df.reset_index(),
            pagination="remote",
            page_size=20,
            sizing_mode="stretch_width",
        )

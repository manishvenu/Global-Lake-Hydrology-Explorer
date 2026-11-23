import pandas as pd
import hvplot.pandas  # noqa: F401
import panel as pn


class WaterBalanceDisplay:
    """Water balance panel for the LIME dashboard.

    Reads the water_balance CSV written by CLAY and produces:
      1. A line chart of P, E, and P−E (all in mm/month)
      2. A line chart of river inflow (m³/month) if available
      3. A line chart of water surface elevation (m) if available
      4. A hypsometric curve (area vs. elevation) if DAHITI hypsometry data exists
    """

    def __init__(self, csv_path: str, hyps_path: str | None = None):
        self.df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        self.df.index.name = "Date"
        self.hyps_df = None
        if hyps_path:
            try:
                self.hyps_df = pd.read_csv(hyps_path)
            except Exception:
                pass

    def make_plots(self) -> list:
        plots = []

        # --- P, E, P-E panel ---
        pe_cols = [c for c in ("P_mean_mm_month", "E_mean_mm_month", "P_minus_E_mm_month")
                   if c in self.df.columns]
        if pe_cols:
            plots.append(
                self.df[pe_cols].hvplot.line(
                    ylabel="mm / month",
                    title="Water Balance: Precipitation, Evaporation, and Net P−E",
                    value_label="mm/month",
                    responsive=True,
                    height=300,
                    legend="top_left",
                    grid=True,
                )
            )

        # --- Inflow panel ---
        if "Inflow_mean_m3_month" in self.df.columns:
            plots.append(
                self.df[["Inflow_mean_m3_month"]].hvplot.line(
                    ylabel="m³ / month",
                    title="River Inflow (GloFAS / NWM)",
                    responsive=True,
                    height=250,
                    legend="top_left",
                    grid=True,
                )
            )

        # --- Water level panel ---
        if "Water_Level_mean_m" in self.df.columns:
            plots.append(
                self.df[["Water_Level_mean_m"]].hvplot.line(
                    ylabel="m (ellipsoidal)",
                    title="Water Surface Elevation (DAHITI)",
                    responsive=True,
                    height=250,
                    legend="top_left",
                    grid=True,
                )
            )

        # --- Hypsometric curve panel ---
        if self.hyps_df is not None and not self.hyps_df.empty:
            plots.append(
                self.hyps_df.hvplot.line(
                    x="water_surface_elevation_m",
                    y="surface_area_km2",
                    xlabel="Water Surface Elevation (m)",
                    ylabel="Surface Area (km²)",
                    title="Hypsometric Curve — Area vs. Elevation (DAHITI)",
                    responsive=True,
                    height=300,
                    grid=True,
                ).opts(tools=["hover"])
            )

        return plots

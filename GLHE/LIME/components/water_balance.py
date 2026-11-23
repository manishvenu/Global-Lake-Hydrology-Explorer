"""
Water balance tab: the lake's net water budget from CLAY's
``{lake}_water_balance.csv`` (``WATER_BALANCE_DATA`` in config.json) and, when
DAHITI provides it, the hypsometric curve (``HYPSOMETRY_DATA``).

Panels, each shown only when its data exists:
  1. Precipitation, evaporation and net P − E (mm/month, ensemble means)
  2. River inflow (m³/month, GloFAS / NWM ensemble mean)
  3. Water surface elevation (m, DAHITI)
  4. Hypsometric curve: surface area against water surface elevation
"""

from __future__ import annotations

import logging

import holoviews as hv
import pandas as pd
import panel as pn
from bokeh.models import NumeralTickFormatter

from GLHE.LIME import theme
from GLHE.LIME.components.common import NO_DATA_MESSAGE, empty_state, is_dark

logger = logging.getLogger(__name__)

CHART_HEIGHT = 260
#: Inflow volumes are plotted in millions of m³ (as in the Time series tab).
VOLUME_SCALE = 1e-6

#: water_balance.csv column -> (legend label, single-letter code for colour).
PE_COLUMNS = {
    "P_mean_mm_month": ("Precipitation", "p"),
    "E_mean_mm_month": ("Evaporation", "e"),
    "P_minus_E_mm_month": ("P − E", None),
}


def read_water_balance(path) -> pd.DataFrame | None:
    """The water balance CSV with a Date index, or None if missing/unreadable."""
    if path is None:
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
    except Exception as exc:
        logger.warning("Could not read water balance %s: %s", path, exc)
        return None
    df.index.name = "Date"
    return df if not df.empty else None


def read_hypsometry(path) -> pd.DataFrame | None:
    """DAHITI hypsometry (elevation vs area), or None if missing/unreadable."""
    if path is None:
        return None
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        logger.warning("Could not read hypsometry %s: %s", path, exc)
        return None
    needed = {"water_surface_elevation_m", "surface_area_km2"}
    return df if needed <= set(df.columns) and not df.empty else None


def _curve(series: pd.Series, vdim: hv.Dimension, label: str, color: str, **opts):
    series = series.dropna()
    return hv.Curve((series.index, series.to_numpy()), "Date", vdim, label=label).opts(
        color=color, line_width=2, tools=["hover"], **opts
    )


def _chart_opts(title: str, **extra) -> dict:
    return dict(
        title=title,
        responsive=True,
        height=CHART_HEIGHT,
        legend_position="top",
        xlabel="",
        yformatter=NumeralTickFormatter(format="0,0.[0]"),
        **extra,
    )


def water_balance_charts(
    wb: pd.DataFrame | None, hyps: pd.DataFrame | None, dark: bool
) -> list:
    """The HoloViews charts for whatever data is present (may be empty)."""
    palette = theme.PALETTE["dark" if dark else "light"]
    net_color = theme.COLORS["dark" if dark else "light"]["text"]
    charts = []

    if wb is not None:
        pe = [c for c in PE_COLUMNS if c in wb.columns and wb[c].notna().any()]
        if pe:
            vdim = hv.Dimension("depth", label="mm/month")
            curves = []
            for col in pe:
                label, slc = PE_COLUMNS[col]
                color = palette[slc] if slc else net_color
                dash = "dashed" if slc is None else "solid"
                curves.append(_curve(wb[col], vdim, label, color, line_dash=dash))
            zero = hv.HLine(0).opts(color=net_color, alpha=0.3, line_width=1)
            charts.append(
                (hv.Overlay(curves) * zero).opts(
                    **_chart_opts("Precipitation, evaporation and net P − E"),
                    legend_cols=len(curves),
                )
            )

        col = "Inflow_mean_m3_month"
        if col in wb.columns and wb[col].notna().any():
            vdim = hv.Dimension("volume", label="million m³/month")
            charts.append(
                hv.Overlay(
                    [_curve(wb[col] * VOLUME_SCALE, vdim, "Inflow", palette["i"])]
                ).opts(**_chart_opts("River inflow (GloFAS / NWM)"))
            )

        col = "Water_Level_mean_m"
        if col in wb.columns and wb[col].notna().any():
            vdim = hv.Dimension("level", label="m (ellipsoidal)")
            charts.append(
                hv.Overlay([_curve(wb[col], vdim, "Water level", palette["l"])]).opts(
                    **_chart_opts("Water surface elevation (DAHITI)")
                )
            )

    if hyps is not None:
        h = hyps.sort_values("water_surface_elevation_m")
        charts.append(
            hv.Curve(
                (h["water_surface_elevation_m"], h["surface_area_km2"]),
                hv.Dimension("elev", label="Water surface elevation (m)"),
                hv.Dimension("area", label="Surface area (km²)"),
            ).opts(
                color=palette["l"],
                line_width=2,
                tools=["hover"],
                title="Hypsometric curve: area vs. elevation (DAHITI)",
                responsive=True,
                height=CHART_HEIGHT,
                yformatter=NumeralTickFormatter(format="0,0.[0]"),
            )
        )
    return charts


def water_balance_view(state) -> pn.viewable.Viewable:
    """The tab's content for the lake currently loaded in ``state``."""
    if not state.output_dir:
        return empty_state(NO_DATA_MESSAGE)
    wb = read_water_balance(state.path_for("WATER_BALANCE_DATA"))
    hyps = read_hypsometry(state.path_for("HYPSOMETRY_DATA"))
    charts = water_balance_charts(wb, hyps, is_dark())
    if not charts:
        return empty_state(
            "No water balance for this lake yet. Re-run CLAY to generate it."
        )
    layout = hv.Layout(charts).cols(1).opts(shared_axes=False)
    return pn.pane.HoloViews(layout, sizing_mode="stretch_width")


def make_water_balance(state) -> pn.viewable.Viewable:
    """Water balance tab for an ``AppState``; redraws when a lake is loaded."""
    return pn.panel(
        pn.bind(
            lambda *_: water_balance_view(state),
            state.param.output_dir,
            state.param.config,
        ),
        sizing_mode="stretch_width",
    )

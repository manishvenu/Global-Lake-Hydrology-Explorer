"""
Time-series tab: one chart per variable (P, E, inflow, outflow).

Features
--------
* Product toggle (CheckButtonGroup): show/hide ERA5-Land, CRUTS, NWM, ...
* Monthly values or calendar-year totals (RadioButtonGroup).
* All charts share the same date axis: zooming one zooms the others. A small
  "navigator" chart underneath has a draggable range box (RangeToolLink).
* Colours follow the variable (``theme.PALETTE``); line dash follows the
  product (``theme.PRODUCT_DASH``), so the same product looks the same
  everywhere.

Ported from the old ``series_data_display.SeriesDataDisplay.make_plots``.
"""

from __future__ import annotations

import holoviews as hv
from bokeh.models import NumeralTickFormatter
import pandas as pd
import panel as pn
from holoviews.plotting.links import RangeToolLink

from GLHE.LIME import theme
from GLHE.LIME.components.common import (
    NO_DATA_MESSAGE,
    columns_for,
    empty_state,
    is_dark,
    products_in,
    slc_axis_label,
    split_column,
)
from GLHE.LIME.components.overview import annual_totals

MONTHLY = "Monthly"
ANNUAL = "Annual totals"
CHART_HEIGHT = 260
VOLUME_SCALE = 1e-6  # m³ -> million m³ for inflow/outflow charts


def aggregate(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    """Return ``df`` unchanged (monthly) or as calendar-year totals."""
    if mode == ANNUAL:
        return pd.DataFrame({c: annual_totals(df[c]) for c in df.columns})
    return df


def variable_chart(
    df: pd.DataFrame, slc: str, products: list[str], mode: str, dark: bool
) -> hv.Overlay | None:
    """
    Overlay of one curve per selected product for a single variable.

    Returns None when no selected product has this variable, so the caller
    can skip the chart entirely.
    """
    cols = [c for c in columns_for(df, slc) if split_column(c)[1] in products]
    if not cols:
        return None
    # The chart title names the variable, so the y-axis only needs the unit
    # (a long "Evapotranspiration (mm/month)" label gets clipped).
    ylabel = theme.SLC_UNITS.get(slc) or slc_axis_label(slc)
    # Inflow/outflow volumes are hundreds of millions of m³; plot them in
    # millions of m³ so the axis reads "600" instead of "600,000,000".
    scale = VOLUME_SCALE if slc in ("i", "o") else 1.0
    if scale != 1.0:
        ylabel = ylabel.replace("m³", "million m³")
    if mode == ANNUAL:
        ylabel = ylabel.replace("/month", "/yr")
    # One shared value dimension per variable keeps curves on a single y-axis,
    # while different variables get separate y-axes in the layout.
    vdim = hv.Dimension(slc, label=ylabel)
    curves = []
    for col in cols:
        series = df[col].dropna()
        product = split_column(col)[1]  # legend label
        curves.append(
            hv.Curve(
                (series.index, series.to_numpy() * scale), "Date", vdim, label=product
            ).opts(
                color=theme.column_color(col, dark),
                line_dash=theme.column_dash(col),
                line_width=2,
                tools=["hover"],
            )
        )
    return hv.Overlay(curves).opts(
        title=theme.SLC_LABELS.get(slc, slc),
        responsive=True,
        height=CHART_HEIGHT,
        # Legend above the plot area so it never covers the curves.
        legend_position="top",
        legend_cols=len(cols),
        xlabel="",
        yformatter=NumeralTickFormatter(format="0,0.[0]"),
    )


def navigator(df: pd.DataFrame, dark: bool) -> hv.Area:
    """Small area chart of the first column, used as the range selector."""
    col = df.columns[0]
    series = df[col].dropna()
    color = theme.column_color(col, dark)
    # A *different* x dimension name than the charts ("Date"), so the
    # navigator keeps showing the full record while the charts zoom.
    xdim = hv.Dimension("nav_date", label="Date")
    return hv.Area((series.index, series.to_numpy()), xdim, "nav").opts(
        color=color,
        alpha=0.35,
        line_color=color,
        height=130,
        responsive=True,
        yaxis=None,
        toolbar=None,
        default_tools=[],
        active_tools=[],  # no tools here; the RangeTool box is added by the link
        title="Drag the box to choose a date range",
        fontsize={"title": "9pt"},
    )


def series_layout(df: pd.DataFrame, products: list[str], mode: str, dark: bool):
    """Stacked, x-linked charts for every variable, plus the navigator."""
    data = aggregate(df, mode)
    charts = [
        chart
        for slc in ("p", "e", "i", "o", "l")
        if (chart := variable_chart(data, slc, products, mode, dark)) is not None
    ]
    if not charts:
        return empty_state("Select at least one product to plot.", height=200)
    nav = navigator(data[[c for c in data.columns if data[c].notna().any()]], dark)
    # Charts in one Layout with the same "Date" dimension share their x-range,
    # so linking the navigator to the first chart moves all of them.
    RangeToolLink(nav, charts[0].values()[0], axes=["x"])
    layout = hv.Layout(charts + [nav]).cols(1).opts(shared_axes=True)
    return pn.pane.HoloViews(layout, linked_axes=False)


def _label(text: str) -> pn.pane.HTML:
    """Small muted label placed in front of a button group."""
    return pn.pane.HTML(
        f'<span class="glhe-kpi-label">{text}</span>',
        margin=(14, 4, 0, 10),
        sizing_mode=None,
        width=70,
    )


def _build(df: pd.DataFrame | None):
    """Controls + charts for one DataFrame (rebuilt when the lake changes)."""
    if df is None or df.empty or not df.notna().any().any():
        return empty_state(NO_DATA_MESSAGE)
    dark = is_dark()
    products = products_in(df)
    product_toggle = pn.widgets.CheckButtonGroup(
        label="Products",
        options=products,
        value=list(products),
        color="primary",
        variant="outline",
        sizing_mode=None,
    )
    mode_toggle = pn.widgets.RadioButtonGroup(
        label="Aggregation",
        options=[MONTHLY, ANNUAL],
        value=MONTHLY,
        color="primary",
        variant="outline",
        sizing_mode=None,
    )
    charts = pn.bind(series_layout, df, product_toggle, mode_toggle, dark)
    # FlexBox (not Row) so the two groups wrap on narrow screens.
    controls = pn.FlexBox(
        pn.Row(_label("Products"), product_toggle, sizing_mode=None),
        pn.Row(_label("Show"), mode_toggle, sizing_mode=None),
        gap="0 24px",
        sizing_mode="stretch_width",
    )
    return pn.Column(
        controls,
        pn.panel(charts, sizing_mode="stretch_width"),
        sizing_mode="stretch_width",
    )


def make_series(state) -> pn.viewable.Viewable:
    """Time-series tab for an ``AppState``; rebuilds when ``state.df`` changes."""
    return pn.panel(pn.bind(_build, state.param.df), sizing_mode="stretch_width")

"""
Overview tab: headline numbers for the selected lake.

Shows
-----
* Summary tiles: lake area (if the polygon is known) and the record period.
* One KPI tile per series column (e.g. "Precipitation - ERA5-Land") with the
  mean annual total and a sparkline of annual totals.
* One P - E tile per product that has both precipitation and evaporation.
* A mean-monthly climatology chart (average January, February, ...).

The numeric work lives in small pure functions (``annual_totals``,
``mean_annual``, ``lake_area_km2``, ``climatology``) so it can be tested
without a browser. ``make_overview`` only arranges widgets.
"""

from __future__ import annotations

import re

import holoviews as hv
import numpy as np
import pandas as pd
import panel as pn

from GLHE.LIME import theme
from GLHE.LIME.components.common import (
    NO_DATA_MESSAGE,
    columns_for,
    empty_state,
    is_dark,
    split_column,
)

TILE_WIDTH = 230  # smallest tile width; the grid stretches tiles to fill rows

#: Lays the KPI tiles out as an even grid (overrides the Column's flex layout).
TILE_GRID_CSS = f"""
:host {{ display: grid !important;
  grid-template-columns: repeat(auto-fill, minmax(min({TILE_WIDTH - 20}px, 100%), 1fr));
  gap: 12px; align-items: stretch; margin: 6px 0 10px; }}
"""
MONTH_NAMES = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()

# ---------------------------------------------------------------------------
# Pure calculations
# ---------------------------------------------------------------------------


def annual_totals(series: pd.Series) -> pd.Series:
    """
    Sum a monthly series into calendar-year totals.

    Years with fewer than 12 valid months are dropped (NaN), so a partial
    first or last year does not look like a drought.
    """
    totals = series.resample("YS").sum(min_count=12)
    return totals.dropna()


def mean_annual(series: pd.Series) -> float:
    """
    Mean annual total of a monthly series.

    Uses complete years only; if there are none, falls back to
    ``mean monthly value * 12``. Returns NaN for an all-empty series.
    """
    totals = annual_totals(series)
    if len(totals):
        return float(totals.mean())
    if series.notna().any():
        return float(series.mean() * 12)
    return float("nan")


def record_period(df: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    """First and last date with any data, or None if the frame is empty."""
    valid = df.dropna(how="all")
    if valid.empty:
        return None
    return valid.index.min(), valid.index.max()


def lake_area_km2(polygon_geojson: dict) -> float | None:
    """
    Geodesic area of the lake polygon in km^2 (None if no polygon).

    Uses pyproj's WGS84 ellipsoid so it is correct anywhere on Earth
    without picking a projection.
    """
    features = (polygon_geojson or {}).get("features") or []
    if not features:
        return None
    try:
        from pyproj import Geod
        from shapely.geometry import shape

        geod = Geod(ellps="WGS84")
        area_m2 = sum(
            abs(geod.geometry_area_perimeter(shape(f["geometry"]))[0])
            for f in features
            if f.get("geometry")
        )
    except Exception:  # a malformed polygon should not break the tab
        return None
    return area_m2 / 1e6


def p_minus_e(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Monthly P - E for every product with both a 'p.' and an 'e.' column."""
    out = {}
    for p_col in columns_for(df, "p"):
        product = split_column(p_col)[1]
        e_col = f"e.{product}"
        if e_col in df.columns:
            out[product] = df[p_col] - df[e_col]
    return out


def climatology(df: pd.DataFrame) -> pd.DataFrame:
    """Mean value for each calendar month (index 1..12) for every column."""
    clim = df.groupby(df.index.month).mean().reindex(range(1, 13))
    clim.index.name = "month"
    return clim


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _fmt(value: float | None, unit: str = "") -> str:
    """
    Readable number plus unit: 1,234 mm/yr / 12.3 mm/yr / 1.39 km³/yr.

    Volumes in m³ of a million or more are shown in km³ (1 km³ = 1e9 m³), and
    other very large numbers get a "million"/"billion" word, so tiles never
    show scientific notation such as 1.39e+09.
    """
    if value is None or not np.isfinite(value):
        return "n/a"
    if abs(value) >= 1e6 and unit.startswith("m³"):
        value, unit = value / 1e9, "k" + unit
        text = f"{value:,.3g}" if abs(value) < 1000 else f"{value:,.0f}"
    elif abs(value) >= 1e9:
        text = f"{value / 1e9:,.3g} billion"
    elif abs(value) >= 1e6:
        text = f"{value / 1e6:,.3g} million"
    elif abs(value) >= 100:
        text = f"{value:,.0f}"
    else:
        text = f"{value:.1f}"
    return f"{text} {unit}".strip()


def _annual_unit(slc: str) -> str:
    """'mm/month' -> 'mm/yr', so tiles show the right unit for annual totals."""
    unit = theme.SLC_UNITS.get(slc, "")
    return unit.replace("/month", "/yr")


def _tile_html(label: str, value: str, color: str, sub: str = "") -> str:
    """HTML for the text part of a KPI tile (coloured bar + label + value)."""
    sub_html = f'<div class="glhe-muted">{sub}</div>' if sub else ""
    return (
        f'<div style="border-left:3px solid {color};padding-left:10px">'
        f'<div class="glhe-kpi-label" title="{label}" style="white-space:nowrap;'
        f'overflow:hidden;text-overflow:ellipsis">{label}</div>'
        f'<div class="glhe-kpi-value">{value}</div>{sub_html}</div>'
    )


def _sparkline(series: pd.Series, color: str, key: str) -> hv.Curve:
    """A tiny axis-less line chart for a KPI tile."""
    # A unique value-dimension name per sparkline keeps each tile on its own
    # y-range (see also linked_axes=False where the pane is created).
    name = "spark_" + re.sub(r"\W", "_", key)
    data = series.reset_index()
    data.columns = ["year", name]
    vdim = hv.Dimension(name, label="annual total")
    return hv.Curve(data, "year", vdim).opts(
        color=color,
        line_width=2,
        height=46,
        responsive=True,
        xaxis=None,
        yaxis=None,
        toolbar=None,
        default_tools=[],
        active_tools=[],
        show_grid=False,
        show_frame=False,
        tools=["hover"],
    )


def _tile(label, value, color, sub="", spark: pd.Series | None = None):
    """One KPI card: text on top, optional sparkline below."""
    items = [pn.pane.HTML(_tile_html(label, value, color, sub), margin=0)]
    if spark is not None and len(spark) > 1:
        items.append(
            pn.pane.HoloViews(
                _sparkline(spark, color, label), margin=(4, 0, 0, 0), linked_axes=False
            )
        )
    return pn.Column(
        *items,
        css_classes=["glhe-card"],
        margin=0,
        sizing_mode="stretch_width",
    )


# ---------------------------------------------------------------------------
# View builders
# ---------------------------------------------------------------------------


def _summary_tiles(df: pd.DataFrame, polygon_geojson: dict, dark: bool) -> list:
    """Area and record-period tiles."""
    c = theme.COLORS["dark" if dark else "light"]
    tiles = []
    area = lake_area_km2(polygon_geojson)
    tiles.append(
        _tile(
            "Lake area",
            _fmt(area, "km²"),
            c["accent"],
            sub="from lake polygon" if area else "polygon not available",
        )
    )
    period = record_period(df)
    if period:
        start, end = period
        years = (end.year - start.year) + 1
        tiles.append(
            _tile(
                "Record period",
                f"{start:%Y}–{end:%Y}",
                c["accent"],
                sub=f"{start:%b %Y} to {end:%b %Y} · {years} yrs",
            )
        )
    return tiles


def _series_tiles(df: pd.DataFrame, dark: bool) -> list:
    """One tile per CSV column, ordered p, e, i, o."""
    tiles = []
    for slc in ("p", "e", "i", "o"):
        for col in columns_for(df, slc):
            tiles.append(
                _tile(
                    theme.column_label(col),
                    _fmt(mean_annual(df[col]), _annual_unit(slc)),
                    theme.column_color(col, dark),
                    sub="mean annual total",
                    spark=annual_totals(df[col]),
                )
            )
    return tiles


def _balance_tiles(df: pd.DataFrame, dark: bool) -> list:
    """One P - E tile per product with both variables."""
    c = theme.COLORS["dark" if dark else "light"]
    tiles = []
    for product, series in p_minus_e(df).items():
        value = mean_annual(series)
        color = c["success"] if value >= 0 else c["danger"]
        tiles.append(
            _tile(
                f"P − E · {product}",
                _fmt(value, "mm/yr"),
                color,
                sub="mean annual net water balance",
                spark=annual_totals(series),
            )
        )
    return tiles


def climatology_plot(df: pd.DataFrame, dark: bool = True) -> hv.Overlay:
    """Mean monthly climatology: one line per column, x = Jan..Dec."""
    clim = climatology(df)
    curves = []
    for col in clim.columns:
        slc, product = split_column(col)
        if slc not in ("p", "e"):
            continue  # inflow/outflow are on a different scale (m³)
        data = pd.DataFrame({"Month": MONTH_NAMES, "value": clim[col].to_numpy()})
        curves.append(
            hv.Curve(
                data, "Month", ("value", "mm/month"), label=theme.column_label(col)
            ).opts(
                color=theme.column_color(col, dark),
                line_dash=theme.column_dash(col),
                line_width=2,
                tools=["hover"],
            )
        )
    if not curves:
        return hv.Overlay([hv.Curve([])])
    return hv.Overlay(curves).opts(
        title="Mean monthly climatology (P and E)",
        responsive=True,
        height=300,
        legend_position="top",  # above the plot area: never covers the lines
        legend_cols=2,
        ylabel="mm/month",
    )


def _build(df: pd.DataFrame | None, polygon_geojson: dict, lake_name: str):
    """Build the whole overview for one state snapshot."""
    if df is None or df.empty:
        return empty_state(NO_DATA_MESSAGE)
    dark = is_dark()
    header = pn.pane.HTML(
        f'<div class="glhe-muted">Overview</div>'
        f'<h2 style="margin:2px 0 4px">{lake_name.replace("_", " ")}</h2>',
        margin=(0, 6),
    )
    tiles = (
        _summary_tiles(df, polygon_geojson, dark)
        + _series_tiles(df, dark)
        + _balance_tiles(df, dark)
    )
    return pn.Column(
        header,
        # A CSS grid: tiles fill the width, every row has the same height.
        pn.Column(*tiles, stylesheets=[TILE_GRID_CSS], sizing_mode="stretch_width"),
        pn.pane.HoloViews(climatology_plot(df, dark), linked_axes=False),
        sizing_mode="stretch_width",
    )


def make_overview(state) -> pn.viewable.Viewable:
    """
    Overview tab for an ``AppState``.

    Rebuilds automatically when ``state.df``, ``state.polygon_geojson`` or
    ``state.lake_name`` change.
    """
    return pn.panel(
        pn.bind(
            _build,
            state.param.df,
            state.param.polygon_geojson,
            state.param.lake_name,
        ),
        sizing_mode="stretch_width",
    )

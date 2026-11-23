"""
Data tab: the monthly series as a sortable, filterable table, plus downloads.

* Tabulator table with the Date column frozen on the left and a filter box
  in every column header.
* "Download CSV" (all rows) and "Download filtered CSV" (only rows matching
  the current header filters / sort) buttons.
* "Download gridded zip" when CLAY wrote one for this lake.

Ported from the old ``series_data_display.SeriesDataDisplay.make_table``.
"""

from __future__ import annotations

import io

import pandas as pd
import panel as pn

from GLHE.LIME import theme
from GLHE.LIME.components.common import (
    NO_DATA_MESSAGE,
    empty_state,
    is_dark,
    split_column,
)

#: FileDownload buttons do not pick up the template's button colours, so
#: style them like the other primary buttons (solid and outline variants).
SOLID_BUTTON_CSS = """
.bk-btn { background-color: var(--glhe-primary) !important; color: #fff !important;
  border: 1px solid var(--glhe-primary) !important; }
.bk-btn:hover { filter: brightness(1.12); }
.bk-btn a, .bk-btn span { color: inherit !important; }
"""
OUTLINE_BUTTON_CSS = """
.bk-btn { background-color: transparent !important;
  color: var(--glhe-primary-soft) !important;
  border: 1px solid var(--glhe-primary-soft) !important; }
.bk-btn:hover { background-color: var(--glhe-surface-2) !important; }
.bk-btn a, .bk-btn span { color: inherit !important; }
"""

#: Keep hovered/selected rows readable in both themes.
TABLE_CSS = """
.tabulator-row:hover, .tabulator-row.tabulator-selected {
  background-color: var(--glhe-surface-2) !important; }
.tabulator-row:hover .tabulator-cell, .tabulator-row.tabulator-selected .tabulator-cell {
  color: var(--glhe-text) !important; }
"""


def table_frame(df: pd.DataFrame) -> pd.DataFrame:
    """The DataFrame shown in the table: Date as a 'YYYY-MM' text column."""
    out = df.round(3).reset_index()
    out = out.rename(columns={out.columns[0]: "Date"})
    out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m")
    return out


def _csv_bytes(df: pd.DataFrame) -> io.BytesIO:
    """Serialise a DataFrame to an in-memory CSV for FileDownload."""
    buf = io.BytesIO(df.to_csv(index=False).encode("utf-8"))
    buf.seek(0)
    return buf


def column_title(column: str) -> str:
    """Table header for a CSV column: 'p.ERA5-Land' -> 'Precip. · ERA5-Land (mm/month)'."""
    slc, _ = split_column(column)
    unit = theme.SLC_UNITS.get(slc)
    label = theme.column_label(column)
    return f"{label} ({unit})" if unit else label


def make_data_table(df: pd.DataFrame) -> pn.widgets.Tabulator:
    """Themed Tabulator for a series DataFrame (already passed through table_frame)."""
    numeric = [c for c in df.columns if c != "Date"]
    return pn.widgets.Tabulator(
        df,
        # Readable headers; the data (and CSV downloads) keep CLAY's codes.
        titles={c: column_title(c) for c in numeric},
        stylesheets=[TABLE_CSS],
        show_index=False,
        frozen_columns=["Date"],
        header_filters={
            "Date": {"type": "input", "func": "like", "placeholder": "e.g. 2010"},
            **{
                c: {"type": "number", "func": ">=", "placeholder": "≥"} for c in numeric
            },
        },
        pagination="remote",
        page_size=24,
        layout="fit_data_stretch",
        theme="midnight" if is_dark() else "simple",
        disabled=True,  # read-only
        sizing_mode="stretch_width",
    )


def _build(df: pd.DataFrame | None, lake_name: str, zip_path):
    """Table + download buttons for one state snapshot."""
    if df is None or df.empty:
        return empty_state(NO_DATA_MESSAGE)
    lake = lake_name or "lake"
    table = make_data_table(table_frame(df))

    downloads = [
        pn.widgets.FileDownload(
            callback=lambda: _csv_bytes(table.value),
            filename=f"{lake}_monthly_data.csv",
            label="Download CSV",
            color="primary",
            icon="download",
            width=200,
            stylesheets=[SOLID_BUTTON_CSS],
        ),
        pn.widgets.FileDownload(
            # current_view = rows left after header filters and sorting.
            callback=lambda: _csv_bytes(table.current_view),
            filename=f"{lake}_monthly_data_filtered.csv",
            label="Download filtered CSV",
            color="primary",
            variant="outline",
            icon="filter",
            width=200,
            stylesheets=[OUTLINE_BUTTON_CSS],
        ),
    ]
    if zip_path is not None:
        downloads.append(
            pn.widgets.FileDownload(
                file=str(zip_path),
                filename=zip_path.name,
                label="Download gridded zip",
                color="primary",
                variant="outline",
                icon="map",
                embed=False,
                width=220,
                stylesheets=[OUTLINE_BUTTON_CSS],
            )
        )
    return pn.Column(
        # FlexBox wraps the buttons onto a second line on narrow screens.
        pn.FlexBox(*downloads, sizing_mode="stretch_width"),
        table,
        sizing_mode="stretch_width",
    )


def make_table(state) -> pn.viewable.Viewable:
    """Data tab for an ``AppState``; rebuilds when the lake/data change."""

    def _for_state(df, lake_name, config):
        return _build(df, lake_name, state.path_for("GRIDDED_DATA_FOLDER"))

    return pn.panel(
        pn.bind(_for_state, state.param.df, state.param.lake_name, state.param.config),
        sizing_mode="stretch_width",
    )

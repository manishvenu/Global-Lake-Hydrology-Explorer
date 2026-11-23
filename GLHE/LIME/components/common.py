"""
Small helpers shared by the LIME view components (overview, series, gridded,
table). Keep this module tiny: anything component-specific belongs in that
component's own file.
"""

from __future__ import annotations

import pandas as pd
import panel as pn

from GLHE.LIME import theme


def is_dark() -> bool:
    """True when the app was started in dark mode (see ``theme.apply_theme``)."""
    return pn.config.theme == "dark"


def empty_state(message: str, height: int = 240) -> pn.pane.HTML:
    """A muted placeholder card, shown when there is no data to display yet."""
    return pn.pane.HTML(
        f'<div class="glhe-card glhe-muted" style="height:{height - 32}px;'
        "display:flex;flex-direction:column;align-items:center;"
        'justify-content:center;text-align:center;gap:6px">'
        '<div style="font-size:1.6rem;opacity:0.6">&#9711;</div>'
        f"<div>{message}</div></div>",
        height=height,
        sizing_mode="stretch_width",
    )


NO_DATA_MESSAGE = "No lake loaded yet. Pick a processed lake or run CLAY."


def split_column(column: str) -> tuple[str, str]:
    """Split a CSV column such as 'p.ERA5-Land' into ('p', 'ERA5-Land')."""
    slc, _, product = column.partition(".")
    return slc, product or column


def columns_for(df: pd.DataFrame, slc: str) -> list[str]:
    """All columns of ``df`` for one single-letter code, e.g. every 'p.*' column."""
    return [c for c in df.columns if c.startswith(slc + ".")]


def products_in(df: pd.DataFrame) -> list[str]:
    """Product names present in ``df``, in column order, without duplicates."""
    seen: list[str] = []
    for c in df.columns:
        product = split_column(c)[1]
        if product not in seen:
            seen.append(product)
    return seen


def slc_axis_label(slc: str) -> str:
    """Axis label like 'Precipitation (mm/month)'."""
    label = theme.SLC_LABELS.get(slc, slc)
    unit = theme.SLC_UNITS.get(slc)
    return f"{label} ({unit})" if unit else label

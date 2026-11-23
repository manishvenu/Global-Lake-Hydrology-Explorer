"""
LIME design system: colours, fonts, CSS and chart defaults in one place.

Every LIME component should take its colours from here instead of hard-coding
them, so the whole app stays visually consistent. To restyle the app, edit the
tokens below; nothing else should need to change.

Usage
-----
>>> from GLHE.LIME import theme
>>> theme.apply_theme()            # once, before building any widgets
>>> theme.slc_color("p")           # colour for precipitation series
'#4ea8ff'
"""

from __future__ import annotations

import holoviews as hv
import panel as pn

# ---------------------------------------------------------------------------
# Core tokens
# ---------------------------------------------------------------------------

#: Font stack. Inter is loaded from Google Fonts in RAW_CSS.
FONT_FAMILY = "Inter, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"
FONT_URL = (
    "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap"
)

#: Surface / text colours for each theme. "dark" is the default.
COLORS = {
    "dark": {
        "bg": "#0b1220",  # page background (deep navy)
        "surface": "#111a2e",  # cards, sidebar
        "surface_2": "#18233b",  # raised elements, inputs
        "border": "#243250",
        "text": "#e6ecf5",
        "text_muted": "#8a9ab5",
        "grid": "#1f2b45",  # chart grid lines
        "accent": "#2dd4bf",  # teal
        "accent_text": "#04201c",  # text drawn on top of accent
        "warning": "#f5b94a",
        "danger": "#f26d6d",
        "success": "#4ade80",
        "primary": "#0072b5",  # buttons (matches Panel's Bootstrap primary)
        "primary_soft": "#5cb3f0",  # outline-button text/border on navy
    },
    "light": {
        "bg": "#f5f7fb",
        "surface": "#ffffff",
        "surface_2": "#eef2f8",
        "border": "#d8dfeb",
        "text": "#0f172a",
        "text_muted": "#5b6b85",
        "grid": "#e3e8f0",
        "accent": "#0d9488",
        "accent_text": "#ffffff",
        "warning": "#b7791f",
        "danger": "#c53030",
        "success": "#15803d",
        "primary": "#0072b5",
        "primary_soft": "#0072b5",
    },
}

#: Accent colour (teal) for the default dark theme. Convenience alias.
ACCENT = COLORS["dark"]["accent"]

#: Series colours by single-letter code (p/e/i/o), tuned per theme so they
#: stay readable on both backgrounds.
PALETTE = {
    "dark": {
        "p": "#4ea8ff",  # precipitation - blue
        "e": "#ffb547",  # evapotranspiration - amber
        "i": "#4ade80",  # inflow - green
        "o": "#b78cff",  # outflow - purple
    },
    "light": {
        "p": "#1f6fd1",
        "e": "#d97706",
        "i": "#15803d",
        "o": "#7c3aed",
    },
}

#: Human-readable names and units for each single-letter code.
SLC_LABELS = {
    "p": "Precipitation",
    "e": "Evapotranspiration",
    "i": "Inflow",
    "o": "Outflow",
}
#: Short names for tight spots (KPI tiles, table headers, legends).
SLC_SHORT = {
    "p": "Precip.",
    "e": "ET",
    "i": "Inflow",
    "o": "Outflow",
}
SLC_UNITS = {
    "p": "mm/month",
    "e": "mm/month",
    "i": "m³/month",
    "o": "m³/month",
}

#: Line dash style per data product, so two products of the same variable
#: (same colour) can still be told apart. Unknown products fall back to solid.
PRODUCT_DASH = {
    "ERA5-Land": "solid",
    "CRUTS": "dashed",
    "NWM": "dotdash",
    "IMERG": "dotted",
}


def slc_color(slc: str, dark: bool = True) -> str:
    """Return the series colour for a single-letter code ('p', 'e', 'i', 'o')."""
    return PALETTE["dark" if dark else "light"].get(slc, ACCENT)


def column_color(column: str, dark: bool = True) -> str:
    """Return the colour for a CSV column such as 'p.ERA5-Land'."""
    return slc_color(column.split(".", 1)[0], dark)


def column_label(column: str, short: bool = True) -> str:
    """Readable name for a CSV column: 'p.ERA5-Land' -> 'Precip. · ERA5-Land'."""
    slc, _, product = column.partition(".")
    names = SLC_SHORT if short else SLC_LABELS
    return f"{names.get(slc, slc)} · {product}" if product else column


def column_dash(column: str) -> str:
    """Return the bokeh line_dash for a CSV column such as 'e.CRUTS'."""
    product = column.split(".", 1)[-1]
    return PRODUCT_DASH.get(product, "solid")


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------


def _css_vars(name: str) -> str:
    """Turn one entry of COLORS into a block of CSS custom properties."""
    c = COLORS[name]
    p = PALETTE[name]
    lines = [f"  --glhe-{k.replace('_', '-')}: {v};" for k, v in c.items()]
    lines += [f"  --glhe-slc-{k}: {v};" for k, v in p.items()]
    return "\n".join(lines)


#: Global stylesheet. Components can use the --glhe-* variables and the
#: helper classes (.glhe-card, .glhe-kpi, .glhe-muted, .glhe-badge).
RAW_CSS = f"""
@import url('{FONT_URL}');

:root, html[data-theme="dark"] {{
{_css_vars("dark")}
}}
html[data-theme="light"] {{
{_css_vars("light")}
}}

body {{
  font-family: {FONT_FAMILY};
  background: var(--glhe-bg);
  color: var(--glhe-text);
}}

/* Cards used by overview tiles, caveat cards, etc. */
.glhe-card {{
  background: var(--glhe-surface);
  border: 1px solid var(--glhe-border);
  border-radius: 12px;
  padding: 14px 16px;
}}
.glhe-kpi-value {{
  font-size: 1.6rem;
  font-weight: 600;
  line-height: 1.2;
}}
.glhe-kpi-label, .glhe-muted {{
  color: var(--glhe-text-muted);
  font-size: 0.8rem;
  letter-spacing: 0.02em;
}}
.glhe-badge {{
  display: inline-block;
  padding: 1px 8px;
  border-radius: 999px;
  font-size: 0.72rem;
  font-weight: 600;
  background: var(--glhe-surface-2);
  color: var(--glhe-text);
  border: 1px solid var(--glhe-border);
}}
.glhe-badge.warning {{ color: var(--glhe-warning); border-color: var(--glhe-warning); }}
.glhe-badge.danger  {{ color: var(--glhe-danger);  border-color: var(--glhe-danger); }}
.glhe-badge.info    {{ color: var(--glhe-accent);  border-color: var(--glhe-accent); }}
.glhe-accent {{ color: var(--glhe-accent); }}

/* Page layout (Panel's Bootstrap template: #header, #sidebar, #main). */
body, #main {{ background-color: var(--glhe-bg) !important; }}
html body #sidebar {{
  background-color: var(--glhe-surface);
  border-right: 1px solid var(--glhe-border);
}}
#header {{ box-shadow: none !important; border-bottom: 1px solid var(--glhe-border); }}

/* Phones: stack the sidebar above the main area instead of side by side,
   and drop the "GLHE" title so the lake name fits in the header. */
@media (max-width: 768px) {{
  html body #content {{ flex-direction: column; flex-wrap: nowrap;
    overflow-y: auto !important; }}
  html body #sidebar {{ width: 100% !important; max-width: 100% !important;
    height: auto; flex: 0 0 auto; border-right: none;
    border-bottom: 1px solid var(--glhe-border); }}
  html body #main {{ width: 100%; flex: 0 0 auto; height: auto;
    max-height: none !important; overflow: visible; padding: 8px; }}
  html, body, html body #content, html body #main {{ overflow-x: hidden !important; }}
  #header .app-header, .glhe-theme-label {{ display: none; }}
  #header-items {{ margin-left: 0; }}
  /* Start with the sidebar hidden so the globe and results come first; the
     menu button opens it. Bootstrap marks an open sidebar with "show" and
     the template starts with it open, so on phones the meaning is flipped. */
  html body #sidebar.collapse.show {{ display: none !important; }}
  html body #sidebar.collapse:not(.show) {{ display: block !important;
    /* Open as a sheet over the bottom of the page, so it shows wherever
       you scrolled to and never covers the menu button. */
    position: fixed; bottom: 0; left: 0; right: 0; z-index: 1000;
    max-height: 78vh; overflow-y: auto; border-radius: 14px 14px 0 0;
    border-top: 1px solid var(--glhe-border);
    box-shadow: 0 -12px 24px rgba(0, 0, 0, 0.35); }}
}}
"""


def root_css(dark: bool = True) -> str:
    """
    ``:root`` variables for the chosen theme.

    Panel's Bootstrap template marks the page with ``data-bs-theme`` rather
    than ``data-theme``, so RAW_CSS alone would always use the dark tokens.
    ``apply_theme`` adds this block after RAW_CSS so the right set wins.
    """
    css = ":root {\n" + _css_vars("dark" if dark else "light") + "\n}"
    if not dark:
        # The template draws a white menu icon; make it dark on a light header.
        css += "\n#header .navbar-toggler-icon { filter: invert(1); }"
    return css


# ---------------------------------------------------------------------------
# Chart defaults
# ---------------------------------------------------------------------------


def chart_opts(dark: bool = True) -> dict:
    """
    Common bokeh styling for HoloViews/hvplot elements.

    Use it as ``plot.opts(**theme.chart_opts())`` if you need to re-apply it
    on a single element; ``apply_theme`` already sets it as the default.
    """
    c = COLORS["dark" if dark else "light"]

    def _hook(plot, element):
        """Style the underlying bokeh figure: transparent, muted grid, fonts."""
        fig = plot.state
        fig.background_fill_alpha = 0
        fig.border_fill_alpha = 0
        fig.outline_line_alpha = 0
        for axis in fig.axis:
            axis.axis_line_color = c["border"]
            axis.major_tick_line_color = c["border"]
            axis.minor_tick_line_color = None
            axis.major_label_text_color = c["text_muted"]
            axis.axis_label_text_color = c["text_muted"]
            axis.major_label_text_font = FONT_FAMILY
            axis.axis_label_text_font = FONT_FAMILY
            axis.axis_label_text_font_style = "normal"
        for grid in fig.grid:
            grid.grid_line_color = c["grid"]
            grid.grid_line_alpha = 1
        if fig.title is not None:
            fig.title.text_color = c["text"]
            fig.title.text_font = FONT_FAMILY
        # The mouse wheel should scroll the page, not zoom the chart under
        # it; zooming stays available from the chart toolbar.
        fig.toolbar.active_scroll = None
        for legend in fig.legend:
            # A see-through card behind the legend so it never hides data.
            legend.background_fill_color = c["surface"]
            legend.background_fill_alpha = 0.8
            legend.border_line_alpha = 0
            legend.label_text_color = c["text"]
            legend.label_text_font = FONT_FAMILY

    return {"hooks": [_hook], "show_grid": True, "active_tools": ["pan"]}


def apply_theme(dark: bool = True) -> None:
    """
    Load Panel extensions with the LIME CSS and set chart defaults.

    Call once at app start-up, before any components are created.
    """
    pn.extension(
        "tabulator",
        raw_css=[RAW_CSS, root_css(dark)],
        theme="dark" if dark else "default",
        sizing_mode="stretch_width",
        notifications=True,
    )
    hv.extension("bokeh")
    import hvplot.pandas  # noqa: F401  (registers the .hvplot accessor)

    opts = chart_opts(dark)
    for element in ("Curve", "Area", "Scatter", "Bars", "Image", "QuadMesh"):
        hv.opts.defaults(getattr(hv.opts, element)(**opts))
    # Overlays (several curves in one chart) need the "pan, not wheel-zoom"
    # default too, or the mouse wheel zooms instead of scrolling the page.
    hv.opts.defaults(hv.opts.Overlay(active_tools=["pan"]))

"""
Small HTML helpers for the LIME sidebar and header.

These are plain functions that return HTML strings, so they are easy to test
and to restyle. The widgets themselves (buttons, inputs) and the logic that
connects them live in ``GLHE/LIME/app.py``.

All text that might come from a file or a user is escaped with ``html.escape``.
"""

from __future__ import annotations

import html

#: Styles for the sidebar panes. Panel components live in a shadow DOM, so
#: page-level classes do not reach in; the ``--glhe-*`` colour variables do.
SIDEBAR_CSS = """
.glhe-section { font-size: 0.72rem; font-weight: 600; letter-spacing: 0.08em;
  text-transform: uppercase; color: var(--glhe-text-muted); margin: 6px 0 2px; }
.glhe-muted { color: var(--glhe-text-muted); font-size: 0.8rem; }
a { color: var(--glhe-accent); }
.glhe-info { background: var(--glhe-surface-2); border: 1px solid var(--glhe-border);
  border-radius: 10px; padding: 8px 10px; font-size: 0.85rem; line-height: 1.4; }
.glhe-info b { color: var(--glhe-text); }
.glhe-pill { display: inline-flex; align-items: center; gap: 6px; padding: 2px 10px;
  border-radius: 999px; font-size: 0.75rem; font-weight: 600;
  border: 1px solid currentColor; }
.glhe-dot { width: 8px; height: 8px; border-radius: 50%; background: currentColor; }
.glhe-pill.running .glhe-dot { animation: glhe-pulse 1.2s ease-in-out infinite; }
@keyframes glhe-pulse { 50% { opacity: 0.25; } }
.glhe-log { display: flex; flex-direction: column-reverse; max-height: 260px;
  overflow: auto; margin: 0; padding: 8px; border-radius: 8px;
  background: var(--glhe-bg); border: 1px solid var(--glhe-border);
  font: 11px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace;
  color: var(--glhe-text-muted); white-space: pre-wrap; word-break: break-all; }
.glhe-title { display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
  font-size: 1.05rem; font-weight: 600; color: var(--glhe-text); }
.glhe-id { font-size: 0.75rem; font-weight: 600; padding: 1px 8px;
  border-radius: 999px; border: 1px solid var(--glhe-accent);
  color: var(--glhe-accent); }
"""

#: Colour variable and label for each job status.
STATUS_STYLE = {
    "idle": ("--glhe-text-muted", "Idle"),
    "running": ("--glhe-accent", "Running"),
    "done": ("--glhe-success", "Finished"),
    "failed": ("--glhe-danger", "Failed"),
}


def section(title: str) -> str:
    """Small upper-case section heading."""
    return f'<div class="glhe-section">{html.escape(title)}</div>'


def status_pill_html(status: str, detail: str = "") -> str:
    """Coloured pill showing a job status, with optional muted detail text."""
    var, label = STATUS_STYLE.get(status, STATUS_STYLE["idle"])
    extra = f' <span class="glhe-muted">{html.escape(detail)}</span>' if detail else ""
    return (
        f'<span class="glhe-pill {html.escape(status)}" style="color:var({var})">'
        f'<span class="glhe-dot"></span>{label}</span>{extra}'
    )


def log_html(lines: list[str], max_lines: int = 300) -> str:
    """
    The last ``max_lines`` lines of a job log as a scrolling block.

    The block uses ``flex-direction: column-reverse`` so the browser keeps it
    scrolled to the newest line without any JavaScript.
    """
    if not lines:
        return '<div class="glhe-muted">No CLAY run yet.</div>'
    text = html.escape("\n".join(lines[-max_lines:]))
    return f'<pre class="glhe-log"><span>{text}</span></pre>'


def lake_info_html(selected: dict | None, lookup_available: bool) -> str:
    """
    Describe the lake picked by ID or by clicking the globe.

    ``selected`` is a ``lake_lookup`` result (``hylak_id``, ``name``,
    ``area_km2``) or just ``{"hylak_id": ...}`` when HydroLAKES is missing.
    """
    if not selected:
        return (
            '<div class="glhe-muted">Type a HYLAK_ID or click a lake on the '
            "globe. IDs: "
            '<a href="https://glhe-fe.projects.earthengine.app/view/'
            'globallakehydrologyexplorer" target="_blank" rel="noopener">'
            "lake ID lookup</a></div>"
        )
    hylak_id = selected.get("hylak_id")
    name = selected.get("name")
    head = '<div class="glhe-section" style="margin:0 0 2px">Selected lake</div>'
    id_txt = f"HYLAK_ID {hylak_id}" if hylak_id is not None else "HYLAK_ID unknown"
    if name:
        area = selected.get("area_km2")
        area_txt = f" &middot; {area:,.1f} km&sup2;" if area else ""
        return (
            f'<div class="glhe-info">{head}'
            f'<b>{html.escape(str(name).replace("_", " "))}'
            f'</b><br><span class="glhe-muted">{id_txt}{area_txt}'
            "</span></div>"
        )
    note = (
        "Not found in HydroLAKES."
        if lookup_available
        else "HydroLAKES could not be read, so the name is unknown " "until CLAY runs."
    )
    return (
        f'<div class="glhe-info">{head}<b>{id_txt}</b><br>'
        f'<span class="glhe-muted">{note}</span></div>'
    )


def header_title_html(display_name: str, hylak_id: int | None) -> str:
    """Lake name and HYLAK_ID badge for the page header."""
    if not display_name:
        return '<div class="glhe-title glhe-muted">No lake loaded</div>'
    badge = (
        f'<span class="glhe-id">HYLAK_ID {hylak_id}</span>'
        if hylak_id is not None
        else ""
    )
    return f'<div class="glhe-title">{html.escape(display_name)}{badge}</div>'


def lake_button_label(entry: dict) -> str:
    """Text for one processed-lake button, e.g. 'Great Salt Lake  #67'."""
    name = entry["name"].replace("_", " ")
    return f"{name}  #{entry['hylak_id']}" if entry.get("hylak_id") else name

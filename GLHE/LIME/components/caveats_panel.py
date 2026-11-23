"""
Caveats tab: one card per known data caveat that applies to the lake.

The cards are built from ``state.caveats`` (a list of dicts made by
``GLHE.LIME.caveats.find_caveats``) and re-render whenever that list changes.
This module only displays caveats; finding them is ``caveats.py``'s job.

Everything is plain HTML generated in Python. All text from the rule files is
HTML-escaped, and only http/https links are shown.
"""

from __future__ import annotations

import html
from urllib.parse import urlparse

import panel as pn

#: CSS for the cards. It lives here (not in theme.RAW_CSS) because Panel
#: renders each pane inside its own shadow DOM, which page-level classes do
#: not reach. The --glhe-* colour variables do reach it, so colours still
#: come from theme.py.
CAVEATS_CSS = """
:host { width: 100%; }
.cv-summary { display: flex; gap: 8px; align-items: center; flex-wrap: wrap;
  margin: 0 0 12px; color: var(--glhe-text-muted); font-size: 0.85rem; }
.cv-card { background: var(--glhe-surface); border: 1px solid var(--glhe-border);
  border-left: 4px solid var(--cv-level, var(--glhe-accent)); border-radius: 12px;
  padding: 14px 16px; margin: 0 0 12px; color: var(--glhe-text); }
.cv-card.warning { --cv-level: var(--glhe-warning); }
.cv-card.error { --cv-level: var(--glhe-danger); }
.cv-card.info { --cv-level: var(--glhe-accent); }
.cv-head { display: flex; gap: 10px; align-items: baseline; flex-wrap: wrap; }
.cv-title { font-weight: 600; font-size: 1rem; margin: 0; }
.cv-meta { color: var(--glhe-text-muted); font-size: 0.75rem; margin: 4px 0 8px; }
.cv-msg { margin: 0 0 10px; line-height: 1.5; font-size: 0.9rem; }
.cv-instruction { background: var(--glhe-surface-2); border-radius: 8px;
  padding: 8px 12px; margin: 0 0 10px; font-size: 0.88rem; line-height: 1.45; }
.cv-instruction b { color: var(--cv-level, var(--glhe-accent)); }
.cv-refs { margin: 0; padding-left: 18px; font-size: 0.82rem; }
.cv-refs li { margin: 2px 0; }
a { color: var(--glhe-accent); text-decoration: none; }
a:hover { text-decoration: underline; }
.badge { display: inline-block; padding: 1px 8px; border-radius: 999px;
  font-size: 0.72rem; font-weight: 600; text-transform: uppercase;
  letter-spacing: 0.04em; border: 1px solid var(--cv-level, var(--glhe-border));
  color: var(--cv-level, var(--glhe-text)); }
.badge.warning { --cv-level: var(--glhe-warning); }
.badge.error { --cv-level: var(--glhe-danger); }
.badge.info { --cv-level: var(--glhe-accent); }
.cv-empty { color: var(--glhe-text-muted); padding: 24px 4px; font-size: 0.95rem; }
"""

#: Shown when no rule matched.
EMPTY_TEXT = "No known caveats matched — that does not mean the data is free of issues."

#: Valid levels; anything else is drawn as "info".
LEVELS = ("error", "warning", "info")


def _esc(value) -> str:
    """HTML-escape any value (None becomes an empty string)."""
    return html.escape("" if value is None else str(value))


def _is_web_link(url) -> bool:
    """True only for http(s) URLs, so rule files cannot inject javascript: links."""
    try:
        parsed = urlparse(str(url))
    except ValueError:
        return False
    return parsed.scheme in ("http", "https") and bool(parsed.netloc)


def _link(url: str, text: str) -> str:
    """An <a> that opens in a new tab."""
    return (
        f'<a href="{_esc(url)}" target="_blank" rel="noopener noreferrer">'
        f"{_esc(text)}</a>"
    )


def _level(caveat: dict) -> str:
    level = str(caveat.get("level") or "info").lower()
    return level if level in LEVELS else "info"


def _scope_text(caveat: dict) -> str:
    """Short description of where/when a caveat applies, for the meta line."""
    parts = []
    period = caveat.get("time")
    if period:
        start, end = period
        end = "present" if end in ("..", None) else end
        start = "start" if start in ("..", None) else start
        parts.append(f"{start} to {end}")
    parts.append("regional" if caveat.get("geometry") else "everywhere")
    return " · ".join(parts)


def caveat_card_html(caveat: dict) -> str:
    """HTML for one caveat card."""
    level = _level(caveat)
    meta = [_esc(caveat.get("name")), _esc(_scope_text(caveat))]
    if caveat.get("source_file"):
        meta.append(_esc(caveat["source_file"]))
    viz = caveat.get("vizqueryearth_url")
    if viz and _is_web_link(viz):
        meta.append(_link(viz, "open in vizqueryearth ↗"))

    parts = [
        f'<div class="cv-card {level}">',
        '<div class="cv-head">',
        f'<span class="badge {level}">{level}</span>',
        f'<p class="cv-title">{_esc(caveat.get("title"))}</p>',
        "</div>",
        f'<div class="cv-meta">{" · ".join(meta)}</div>',
        f'<p class="cv-msg">{_esc(caveat.get("message"))}</p>',
    ]
    if caveat.get("instruction"):
        parts.append(
            '<div class="cv-instruction"><b>What to do:</b> '
            f'{_esc(caveat["instruction"])}</div>'
        )
    refs = [
        r
        for r in caveat.get("references") or []
        if isinstance(r, dict) and _is_web_link(r.get("url"))
    ]
    if refs:
        items = "".join(
            f"<li>{_link(r['url'], r.get('title') or r['url'])}</li>" for r in refs
        )
        parts.append(f'<ul class="cv-refs">{items}</ul>')
    parts.append("</div>")
    return "".join(parts)


def caveats_html(caveats: list[dict]) -> str:
    """HTML for the whole list: a count summary, then the cards (or empty text)."""
    if not caveats:
        return f'<div class="cv-empty">{_esc(EMPTY_TEXT)}</div>'
    counts = {lvl: 0 for lvl in LEVELS}
    for c in caveats:
        counts[_level(c)] += 1
    badges = "".join(
        f'<span class="badge {lvl}">{n} {lvl}</span>' for lvl, n in counts.items() if n
    )
    summary = (
        f'<div class="cv-summary">{badges}'
        "<span>Known issues from queryearth rule files that match this lake's "
        "location, dates and products.</span></div>"
    )
    return summary + "".join(caveat_card_html(c) for c in caveats)


def make_caveats(state) -> pn.viewable.Viewable:
    """
    The Caveats tab for an ``AppState``.

    Re-renders whenever ``state.caveats`` changes.
    """
    pane = pn.pane.HTML(
        caveats_html(state.caveats),
        stylesheets=[CAVEATS_CSS],
        sizing_mode="stretch_width",
    )

    def _update(event):
        pane.object = caveats_html(event.new)

    state.param.watch(_update, "caveats")
    return pn.Column(pane, sizing_mode="stretch_width")

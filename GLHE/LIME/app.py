"""
LIME app: the Global Lake Hydrology Explorer web interface.

Run it with::

    panel serve GLHE/LIME/app.py --show
    panel serve GLHE/LIME/app.py --show --args            # same thing
    # open a lake directly:
    #   http://localhost:5006/app?output_dir=/path/to/LakeOutputDirectory/Great_Salt_Lake
    # or set GLHE_OUTPUT_DIR=/path/to/lake/folder before serving.

From Python: ``GLHE.LIME.app.serve(output_dir)``.

How the pieces fit
------------------
* ``AppState`` (state.py) holds the loaded lake: its folder, data and polygon.
  Every tab (components/*.py) watches the state and redraws itself.
* ``LimeApp`` (below) builds the page and connects things that need more than
  one part of the app: the sidebar (pick a lake, run CLAY), the globe, the
  caveats and the header.
* CLAY runs in a separate process (jobs.py), one run per server. Every page
  checks on it once a second with a Panel periodic callback, which keeps all
  widget updates on the page's own thread (the job's background thread never
  touches widgets). Only the page that started a run opens its result.
"""

from __future__ import annotations

import html
import logging
import os
from pathlib import Path

import panel as pn
import param

from GLHE.LIME import catalog, caveats, jobs, lake_lookup, theme
from GLHE.LIME.components import sidebar as sb
from GLHE.LIME.components.caveats_panel import make_caveats
from GLHE.LIME.components.globe import LakeGlobe, read_points
from GLHE.LIME.components.gridded import make_gridded
from GLHE.LIME.components.overview import make_overview
from GLHE.LIME.components.series import make_series
from GLHE.LIME.components.table import make_table
from GLHE.LIME.state import AppState

logger = logging.getLogger(__name__)

#: Environment variable naming a lake folder to open at start-up.
OUTPUT_DIR_ENV = "GLHE_OUTPUT_DIR"

#: How often (milliseconds) each page checks on the shared CLAY job. The
#: check is cheap and only touches widgets when something changed.
POLL_MS = 1000

#: Index of the Gridded tab (cells are drawn on the globe only while it is open).
GRIDDED_TAB = 2
#: Index of the Caveats tab (its title carries the number of caveats).
CAVEATS_TAB = 3

#: Largest HYLAK_ID in HydroLAKES v1.0.
MAX_HYLAK_ID = 1427688

#: Tab bar: wrap onto two lines on narrow screens instead of hiding tabs.
TABS_CSS = """
.bk-header, .bk-headers-wrapper, .bk-headers { flex-wrap: wrap; }
.bk-tab { white-space: nowrap; }
"""

#: Caption under the globe describing the gridded cells drawn on it.
CELLS_CAPTION_CSS = """
.glhe-cells { font-size: 0.78rem; color: var(--glhe-text-muted); }
.glhe-cells b { color: var(--glhe-text); font-weight: 600; }
.glhe-ramp { display: inline-block; width: 90px; height: 8px; border-radius: 4px;
  vertical-align: middle; margin: 0 6px;
  background: linear-gradient(90deg, #440154, #3b528b, #21918c, #5ec962, #fde725); }
"""

# One CLAY job shared by every browser session on this server: CLAY writes to
# a shared folder and uses module-level settings, so two runs at once would
# trip over each other. Every open page shows the status of this one job.
# It is kept in ``pn.state.cache`` because ``panel serve`` re-runs this file
# for every new page, so a plain module-level variable would not be shared.
JOB_CACHE_KEY = "glhe_clay_job"


def shared_job() -> jobs.ClayJob:
    """The server-wide CLAY job (created on first use)."""
    if JOB_CACHE_KEY not in pn.state.cache:
        pn.state.cache[JOB_CACHE_KEY] = jobs.ClayJob()
    return pn.state.cache[JOB_CACHE_KEY]


def _session_arg(name: str) -> str | None:
    """A ``?name=value`` URL parameter of the current page, if any."""
    args = pn.state.session_args or {}
    values = args.get(name)
    if not values:
        return None
    value = values[0]
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


class CellsRelay(param.Parameterized):
    """
    Stands between the Gridded tab and the globe.

    The Gridded tab writes the selected layer's cells here; the app copies
    them to the globe only while the Gridded tab is open, so the other tabs
    show a clean lake outline.
    """

    cells_geojson = param.Dict(default={})


def cells_caption_html(cells: dict) -> str:
    """One line saying which layer the globe's cells show, with a colour ramp."""
    features = (cells or {}).get("features") or []
    if not features:
        return ""
    props = features[0].get("properties", {})
    slc = props.get("slc", "")
    values = [f["properties"]["value"] for f in features if "value" in f["properties"]]
    label = theme.SLC_LABELS.get(slc, slc)
    unit = theme.SLC_UNITS.get(slc, "")
    date = str(props.get("date", ""))[:7]
    low, high = (min(values), max(values)) if values else (None, None)
    ramp = (
        f'{low:.3g}<span class="glhe-ramp"></span>{high:.3g} {html.escape(unit)}'
        if low is not None
        else ""
    )
    return (
        '<div class="glhe-cells">Globe cells: '
        f"<b>{html.escape(label)} · {html.escape(str(props.get('product', '')))}"
        f"</b> {html.escape(date)} &nbsp; {ramp}</div>"
    )


def _notify(kind: str, message: str) -> None:
    """Show a toast if the page supports it; otherwise just log the message."""
    logger.info("%s: %s", kind, message)
    notifications = pn.state.notifications
    if notifications is not None:
        getattr(notifications, kind)(message, duration=5000)


class LimeApp:
    """
    The whole LIME page and the logic that connects its parts.

    Parameters
    ----------
    output_dir : str, optional
        Lake folder (holding config.json) to open at start-up.
    dark : bool
        Dark theme (True) or light.
    state : AppState, optional
        Shared state; a fresh one is made if omitted.
    job : jobs.ClayJob, optional
        The CLAY job to drive; defaults to the server-wide :func:`shared_job`.
    roots : list of str, optional
        Folders to scan for processed lakes; defaults to the catalog's.
    """

    def __init__(self, output_dir=None, dark=True, state=None, job=None, roots=None):
        self.dark = dark
        self.state = state or AppState()
        self.job = job or shared_job()
        self.roots = list(roots) if roots else None
        self.selected: dict | None = None  # lake picked by ID / globe click
        self._poller = None  # periodic callback that follows the shared job
        self._last_seen = None  # (status, log) last copied into the page
        self._last_status = self.job.status
        self._started_here = False  # did *this* page start the current run?

        self.globe = LakeGlobe(dark=dark, height=460, min_height=320)
        self.globe.param.watch(self._on_globe_click, "clicked")
        self.cells = CellsRelay()
        self.cells_caption = pn.pane.HTML(
            "", stylesheets=[CELLS_CAPTION_CSS], margin=(0, 10), visible=False
        )

        self._build_sidebar()
        self.tabs = self._build_tabs()
        self.header = pn.pane.HTML(
            sb.header_title_html("", None),
            stylesheets=[sb.SIDEBAR_CSS],
            sizing_mode="stretch_width",
            margin=(0, 10),
        )
        self.theme_switch = pn.widgets.Switch(
            value=dark, width=40, margin=(12, 4), align="center"
        )
        self.theme_switch.param.watch(self._on_theme_switch, "value")

        # Things that depend on more than one part of the app.
        self.state.param.watch(self._on_lake_changed, ["df", "polygon_geojson"])
        self.state.param.watch(self._on_files_changed, ["output_dir", "config"])
        self.state.param.watch(self._on_caveats_changed, "caveats")
        self.state.param.watch(self._update_header, ["lake_name", "hylak_id"])

        if output_dir:
            self.load(output_dir)
        self.refresh_catalog()
        self.tabs.param.watch(self._sync_cells, "active")
        self.cells.param.watch(self._sync_cells, "cells_geojson")
        self._sync_cells()
        # Every page follows the shared job, so a run started from another
        # page shows up here too (status, log, disabled Run button).
        self._start_polling()
        self.poll_job()

    # ------------------------------------------------------------------ layout
    def _build_sidebar(self) -> None:
        """Create the sidebar widgets (laid out in :meth:`sidebar`)."""
        css = [sb.SIDEBAR_CSS]
        self.id_input = pn.widgets.IntInput(
            label="HYLAK_ID",
            value=None,
            start=1,
            end=MAX_HYLAK_ID,
            placeholder="e.g. 67",
        )
        self.find_button = pn.widgets.Button(
            label="Find", icon="search", color="primary", align="end", width=90
        )
        self.find_button.on_click(lambda _: self._on_find())
        # Pressing Enter (or leaving the box) after typing an ID also finds it.
        self.id_input.param.watch(self._on_id_typed, "value")
        self.lake_info = pn.pane.HTML(
            sb.lake_info_html(None, self._lookup_available), stylesheets=css
        )

        self.filter_input = pn.widgets.TextInput(
            label="Processed lakes", placeholder="Filter by name"
        )
        self.filter_input.param.watch(lambda _: self._render_catalog(), "value_input")
        self.refresh_button = pn.widgets.Button(
            icon="refresh",
            variant="outline",
            width=40,
            align="end",
            description="Scan the output folders again",
        )
        self.refresh_button.on_click(lambda _: self.refresh_catalog())
        self.catalog_list = pn.Column(
            sizing_mode="stretch_width", max_height=260, scroll=True
        )
        self._catalog: list[dict] = []

        self.run_button = pn.widgets.Button(
            label="Run CLAY", icon="player-play", color="primary", disabled=True
        )
        self.run_button.on_click(lambda _: self.run_clay())
        self.cancel_button = pn.widgets.Button(
            label="Cancel",
            icon="player-stop",
            color="danger",
            variant="outline",
            visible=False,
        )
        self.cancel_button.on_click(lambda _: self.job.cancel())
        self.status_pill = pn.pane.HTML(sb.status_pill_html("idle"), stylesheets=css)
        self.log_pane = pn.pane.HTML(sb.log_html([]), stylesheets=css)
        self.log_card = pn.Card(
            self.log_pane, title="Log", collapsed=True, sizing_mode="stretch_width"
        )

    def sidebar(self) -> list:
        """Sidebar contents: find a lake, processed lakes, run CLAY."""
        css = [sb.SIDEBAR_CSS]
        return [
            pn.pane.HTML(sb.section("Find a lake"), stylesheets=css),
            pn.Row(self.id_input, self.find_button),
            self.lake_info,
            pn.pane.HTML(sb.section("Run CLAY"), stylesheets=css),
            self.run_button,
            pn.Row(self.status_pill, self.cancel_button),
            self.log_card,
            pn.pane.HTML(sb.section("Results"), stylesheets=css),
            pn.Row(self.filter_input, self.refresh_button),
            self.catalog_list,
        ]

    def _build_tabs(self) -> pn.Tabs:
        """The result tabs under the globe."""
        return pn.Tabs(
            ("Overview", make_overview(self.state)),
            ("Time series", make_series(self.state)),
            ("Gridded", make_gridded(self.state, globe=self.cells)),
            (self._caveats_title(), make_caveats(self.state)),
            ("Data", make_table(self.state)),
            dynamic=True,
            stylesheets=[TABS_CSS],
            sizing_mode="stretch_width",
        )

    def main(self) -> list:
        """Main area: the globe on top, result tabs underneath."""
        return [self.globe, self.cells_caption, self.tabs]

    def template(self) -> pn.template.BootstrapTemplate:
        """The full page."""
        c = theme.COLORS["dark" if self.dark else "light"]
        return pn.template.BootstrapTemplate(
            title="GLHE",
            theme="dark" if self.dark else "default",
            header_background=c["surface"],
            header_color=c["text"],
            header=[self.header, pn.HSpacer(), self._theme_label(), self.theme_switch],
            sidebar=self.sidebar(),
            main=self.main(),
            sidebar_width=330,
        )

    def _theme_label(self) -> pn.pane.HTML:
        return pn.pane.HTML(
            '<span style="font-size:0.8rem;opacity:0.7">Dark</span>',
            css_classes=["glhe-theme-label"],
            align="center",
            margin=(0, 0),
            sizing_mode="fixed",
            width=34,
            height=24,
        )

    # --------------------------------------------------------- choosing lakes
    @property
    def _lookup_available(self) -> bool:
        """True when HydroLAKES can be read (local copy or S3)."""
        return lake_lookup.available()

    def _on_find(self) -> None:
        if self.id_input.value:
            self.select_lake(int(self.id_input.value))
        else:
            _notify("warning", "Type a HYLAK_ID first.")

    def _on_id_typed(self, event) -> None:
        current = (self.selected or {}).get("hylak_id")
        if event.new and event.new != current:
            self.select_lake(int(event.new))

    def select_lake(self, hylak_id: int, info: dict | None = None) -> None:
        """
        Pick a lake (by typed ID or a globe click).

        Shows its name and outline when HydroLAKES is available, enables
        "Run CLAY", and opens its results right away if CLAY already
        processed it.
        """
        info = info or lake_lookup.lookup_by_id(hylak_id) or {}
        self.selected = {"hylak_id": int(hylak_id), **info}
        self.id_input.value = int(hylak_id)  # no-op for _on_id_typed: same ID
        self.lake_info.object = sb.lake_info_html(self.selected, self._lookup_available)
        if info.get("geojson") and self.state.hylak_id != hylak_id:
            self.globe.lake_geojson = info["geojson"]
        self._update_run_button()

        entry = next((e for e in self._catalog if e["hylak_id"] == hylak_id), None)
        if entry and self.state.output_dir != entry["output_dir"]:
            self.load(entry["output_dir"], hylak_id=hylak_id)

    def _on_globe_click(self, event) -> None:
        click = event.new or {}
        if "lon" not in click or "lat" not in click:
            return
        info = lake_lookup.lookup_by_point(click["lon"], click["lat"])
        if info is None:
            msg = (
                "No lake found there."
                if self._lookup_available
                else "HydroLAKES could not be read; type a HYLAK_ID instead."
            )
            _notify("info", msg)
            return
        self.select_lake(info["hylak_id"], info)

    # ------------------------------------------------------- loading results
    def load(self, output_dir, hylak_id: int | None = None) -> bool:
        """
        Open a CLAY output folder. Returns False (with a message) on failure.

        Real CLAY folders have no HYLAK_ID or lake polygon in them, so when we
        know the ID (from the catalog, the job or the search box) we add it,
        and fetch the outline from HydroLAKES if it is available.
        """
        try:
            self.state.load_output_dir(output_dir)
        except Exception as exc:  # missing/broken config.json
            logger.exception("Could not load %s", output_dir)
            _notify("error", f"Could not open {output_dir}: {exc}")
            return False
        if self.state.hylak_id is None and hylak_id is not None:
            self.state.hylak_id = int(hylak_id)
        hylak_id = self.state.hylak_id
        if hylak_id is not None:
            # Reuse what we already know about this lake, else ask HydroLAKES
            # (only when the folder has no polygon of its own).
            info = self.selected or {}
            if info.get("hylak_id") != hylak_id:
                info = {"hylak_id": hylak_id}
                if not self.state.polygon_geojson:
                    info.update(lake_lookup.lookup_by_id(hylak_id) or {})
            if not self.state.polygon_geojson and info.get("geojson"):
                self.state.polygon_geojson = info["geojson"]
            info.setdefault("name", self.state.lake_name)  # known from the folder
            self.selected = info
            self.id_input.value = hylak_id  # same ID: does not re-trigger
            self.lake_info.object = sb.lake_info_html(info, self._lookup_available)
        else:
            # The folder does not say which lake it is (true of real CLAY
            # output): forget the previous selection so "Run CLAY" cannot
            # re-run the wrong lake, and say the ID is unknown.
            self.selected = None
            self.id_input.value = None
            self.lake_info.object = sb.lake_info_html(
                {"name": self.state.lake_name, "hylak_id": None},
                self._lookup_available,
            )
        self._update_run_button()
        self._remember_in_url(self.state.output_dir)
        self._render_catalog()
        return True

    def _remember_in_url(self, output_dir: str) -> None:
        """Put ``?output_dir=`` in the address bar so reloads keep the lake."""
        location = pn.state.location
        if location is not None:
            try:
                location.update_query(output_dir=output_dir)
            except Exception:  # not critical
                logger.debug("Could not update the URL", exc_info=True)

    def refresh_catalog(self) -> None:
        """Scan the output folders for processed lakes and redraw the list."""
        extra = []
        if self.state.output_dir:  # always list the lake that is open
            extra.append(Path(self.state.output_dir).parent)
        if self.roots is None:
            self._catalog = catalog.list_processed_lakes(None, extra_roots=extra)
        else:
            roots = self.roots + extra
            self._catalog = catalog.list_processed_lakes(roots[0], roots[1:])
        self._render_catalog()

    def _render_catalog(self) -> None:
        """One button per processed lake, filtered by the text box."""
        text = (self.filter_input.value_input or "").strip().lower()
        entries = [
            e
            for e in self._catalog
            if text in e["name"].replace("_", " ").lower() or text == str(e["hylak_id"])
        ]
        if not entries:
            msg = "No processed lakes found." if not text else "No match."
            self.catalog_list.objects = [
                pn.pane.HTML(
                    f'<div class="glhe-muted">{msg}</div>', stylesheets=[sb.SIDEBAR_CSS]
                )
            ]
            return
        buttons = []
        for entry in entries:
            is_open = entry["output_dir"] == self.state.output_dir
            button = pn.widgets.Button(
                label=sb.lake_button_label(entry),
                icon="droplet",
                color="primary" if is_open else "default",
                variant="solid" if is_open else "outline",
                sizing_mode="stretch_width",
                description=entry["output_dir"],
            )
            button.on_click(
                lambda _, e=entry: self.load(e["output_dir"], e["hylak_id"])
            )
            buttons.append(button)
        self.catalog_list.objects = buttons

    # ------------------------------------------------- cross-component wiring
    def _on_lake_changed(self, *_events) -> None:
        """New data or outline: move the globe and recompute the caveats."""
        self.globe.lake_geojson = self.state.polygon_geojson or {}
        if self.state.df is None and not self.state.polygon_geojson:
            self.state.caveats = []
            return
        try:
            self.state.caveats = caveats.caveats_for_state(self.state)
        except Exception:  # a bad rule must never break loading a lake
            logger.exception("Could not compute caveats")
            self.state.caveats = []

    def _on_files_changed(self, *_events) -> None:
        """New lake folder: show its NWM point (if CLAY wrote one)."""
        self.globe.points_geojson = read_points(
            self.state.path_for("NWM_LAKE_POINT_SHAPEFILENAME")
        )

    def _caveats_title(self) -> str:
        n = len(self.state.caveats)
        return f"Caveats ({n})" if n else "Caveats"

    def _on_caveats_changed(self, event) -> None:
        """Update the Caveats tab title and the caveat areas on the globe."""
        self.globe.caveat_geojson = caveats.caveats_to_geojson(event.new)
        self.tabs[CAVEATS_TAB] = (self._caveats_title(), self.tabs[CAVEATS_TAB])

    def _sync_cells(self, *_events) -> None:
        """Draw gridded cells on the globe only while the Gridded tab is open."""
        cells = self.cells.cells_geojson if self.tabs.active == GRIDDED_TAB else {}
        self.globe.cells_geojson = cells or {}
        caption = cells_caption_html(cells)
        self.cells_caption.object = caption
        self.cells_caption.visible = bool(caption)

    def _update_header(self, *_events) -> None:
        self.header.object = sb.header_title_html(
            self.state.display_name, self.state.hylak_id
        )

    def _on_theme_switch(self, event) -> None:
        """
        Switch dark/light. The globe switches at once; the rest of the page
        is styled when it is built, so the page reloads with ``?theme=``.
        """
        self.globe.dark = event.new
        location = pn.state.location
        if location is not None:
            location.reload = True
            location.update_query(theme="dark" if event.new else "light")

    # ------------------------------------------------------------ CLAY job
    def _update_run_button(self) -> None:
        hylak_id = (self.selected or {}).get("hylak_id")
        running = self.job.is_running
        self.run_button.disabled = hylak_id is None or running
        self.run_button.label = (
            f"Run CLAY for #{hylak_id}" if hylak_id is not None else "Run CLAY"
        )
        self.cancel_button.visible = running

    def run_clay(self) -> None:
        """Start CLAY for the selected lake and follow its progress."""
        hylak_id = (self.selected or {}).get("hylak_id")
        if hylak_id is None:
            _notify("warning", "Pick a lake first.")
            return
        if not self.job.start(hylak_id):
            _notify("warning", "CLAY is already running; wait for it to finish.")
            return
        self._started_here = True  # only this page opens the result when done
        self.log_card.collapsed = False
        self._start_polling()
        self.poll_job()

    def _start_polling(self) -> None:
        """Check on the job every POLL_MS ms (only possible in a live page).

        The callback stays on for the life of the page; Panel stops it when
        the browser tab closes.
        """
        if self._poller is None and pn.state.curdoc is not None:
            self._poller = pn.state.add_periodic_callback(self.poll_job, POLL_MS)

    def poll_job(self) -> None:
        """Copy the job's status and log into the page; react when it ends."""
        job = self.job
        status, log = job.status, job.log
        if (status, log) != self._last_seen:  # only touch widgets on a change
            self._last_seen = (status, log)
            detail = f"#{job.hylak_id}" if job.hylak_id is not None else ""
            self.status_pill.object = sb.status_pill_html(status, detail)
            self.log_pane.object = sb.log_html(log)
            self.state.param.update(job_status=status, job_log="\n".join(log))
            self._update_run_button()

        finished = self._last_status == "running" and status != "running"
        self._last_status = status
        if not finished:
            return
        started_here, self._started_here = self._started_here, False
        if not started_here:
            # Another page ran CLAY: list its result, but do not replace the
            # lake this viewer is looking at.
            if status == "done":
                self.refresh_catalog()
            return
        if status == "done" and job.output_dir:
            if self.load(job.output_dir, hylak_id=job.hylak_id):
                _notify("success", f"CLAY finished: {self.state.display_name}")
            self.refresh_catalog()
        elif status == "done":
            _notify("warning", "CLAY finished but did not report an output folder.")
        else:
            _notify("error", "CLAY failed; see the log.")


def build_app(output_dir=None, state=None, dark=None, job=None, roots=None):
    """
    Build the LIME page.

    Parameters
    ----------
    output_dir : str, optional
        Lake folder to open first. If omitted, the ``?output_dir=`` URL
        parameter or the ``GLHE_OUTPUT_DIR`` environment variable is used.
    state : AppState, optional
        Shared state (a new one if omitted).
    dark : bool, optional
        Theme. If omitted, ``?theme=light`` in the URL selects light;
        dark is the default.
    job, roots
        Passed to :class:`LimeApp` (mainly for tests).

    Returns
    -------
    pn.template.BootstrapTemplate
        Call ``.servable()`` or ``.show()`` on it. The ``LimeApp`` that runs
        it is available as ``template.lime``.
    """
    if dark is None:
        dark = (_session_arg("theme") or "dark") == "dark"
    output_dir = (
        output_dir or _session_arg("output_dir") or os.environ.get(OUTPUT_DIR_ENV)
    )
    theme.apply_theme(dark=dark)
    app = LimeApp(output_dir=output_dir, dark=dark, state=state, job=job, roots=roots)
    template = app.template()
    template.lime = app  # handy for scripts and tests
    return template


def serve(output_dir=None, port: int = 5006, address: str | None = None, show=True):
    """
    Serve the app from Python and block until stopped (Ctrl+C).

    Same as ``panel serve GLHE/LIME/app.py``; ``output_dir`` is the CLAY lake
    folder (the one holding config.json) to open first.
    """
    return pn.serve(
        lambda: build_app(output_dir=output_dir),
        port=port,
        address=address,
        show=show,
        title="GLHE · Lake Explorer",
    )


# `panel serve GLHE/LIME/app.py` runs this file with a __name__ like
# "bokeh_app_<hash>"; build one page per browser session.
def warm_hydrolakes() -> None:
    """Load HydroLAKES' bounding-box index in the background, once per server,
    so the first globe click is fast even when HydroLAKES is read from S3."""
    if pn.state.cache.get("glhe_hydrolakes_warm"):
        return
    pn.state.cache["glhe_hydrolakes_warm"] = True

    def _warm():
        try:
            from GLHE.CLAY import hydrolakes

            if hydrolakes.available():
                hydrolakes._bbox_index(hydrolakes.source())
        except Exception as exc:  # the app still works; clicks just start slower
            logger.warning("Could not preload HydroLAKES: %s", exc)

    import threading

    threading.Thread(target=_warm, daemon=True).start()


if __name__.startswith("bokeh"):
    warm_hydrolakes()
    build_app().servable(title="GLHE · Lake Explorer")

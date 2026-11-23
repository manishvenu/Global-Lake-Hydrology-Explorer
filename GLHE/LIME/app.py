"""
LIME: the GLHE web app.

    panel serve GLHE/LIME/app.py        (or GLHE.LIME.app.serve(folder))

Sidebar: find a lake (search box, or click it on the globe), run CLAY on it,
or open a lake that was already processed.
Main area: the globe, then tabs with the open lake's results.

Open a lake folder directly with ?output_dir=/path/to/lake or the
GLHE_OUTPUT_DIR environment variable.
"""

from __future__ import annotations

import io
import json
import logging
import os
import threading
from pathlib import Path, PureWindowsPath

import hvplot.pandas  # noqa: F401  (adds df.hvplot)
import pandas as pd
import panel as pn
from panel.io.state import set_curdoc

from GLHE.CLAY import caveats, hydrolakes
from GLHE.CLAY.globals import SLC_MAPPING_REVERSE_UNITS
from GLHE.LIME import catalog, gridded, jobs, public
from GLHE.LIME.globe import LakeGlobe, lake_tiles_url

# Wider, darker scrollbars so the scrollable sidebar and main area are obvious.
SCROLLBAR_CSS = """
* { scrollbar-width: auto; scrollbar-color: #888 #e6e6e6; }
::-webkit-scrollbar { width: 14px; height: 14px; }
::-webkit-scrollbar-track { background: #e6e6e6; }
::-webkit-scrollbar-thumb { background: #888; border-radius: 7px; }
"""
pn.extension("tabulator", raw_css=[SCROLLBAR_CSS])
logger = logging.getLogger(__name__)

SLC_LABELS = {
    "p": "Precipitation",
    "e": "Evapotranspiration",
    "i": "Inflow",
    "o": "Outflow",
    "l": "Water level",
}


# --------------------------------------------------------------------------
# Reading a CLAY output folder
# --------------------------------------------------------------------------
def find_file(value: str | None, folder: Path) -> Path | None:
    """A file named in config.json. CLAY writes absolute paths, which break when
    a folder is moved, so fall back to the same file name inside ``folder``."""
    if not value:
        return None
    name = PureWindowsPath(value).name if "\\" in value else Path(value).name
    for path in (Path(value), folder / value, folder / name):
        for candidate in (path, path.with_name(path.name + ".shp")):
            if candidate.is_file():
                return candidate
    return None


def read_geojson(path: Path | None) -> dict:
    """A shapefile as a GeoJSON dict in EPSG:4326 ({} if missing)."""
    if path is None or not path.exists():
        return {}
    import geopandas as gpd

    return json.loads(gpd.read_file(path).to_crs(4326).to_json())


def load_lake(folder) -> dict:
    """Everything LIME shows about one processed lake."""
    folder = Path(folder).expanduser().resolve()
    config = json.loads((folder / "config.json").read_text())
    csv = find_file(config.get("SERIES_DATA"), folder)
    df = pd.read_csv(csv, index_col=0, parse_dates=True) if csv else None
    polygon = read_geojson(folder / "Lake_Polygon.shp")
    return {
        "folder": str(folder),
        "name": config.get("LAKE_NAME", folder.name),
        "hylak_id": config.get("HYLAK_ID"),
        "df": df,
        "polygon": polygon,
        "points": read_geojson(
            find_file(config.get("NWM_LAKE_POINT_SHAPEFILENAME"), folder)
        ),
        "gridded_zip": find_file(config.get("GRIDDED_DATA_FOLDER"), folder),
        "caveats": caveats.caveats_for_dataset(polygon or None, df),
    }


# --------------------------------------------------------------------------
# Result views: each takes data and returns something Panel can show
# --------------------------------------------------------------------------
def unit_of(name: str) -> str:
    """Unit of a CSV column ('p.ERA5-Land') or gridded file ('p_ERA5-Land_...tif'),
    from the letter code in front: 'mm/month', 'm³/month', ..."""
    code = name.replace("_", ".").split(".")[0]
    return SLC_MAPPING_REVERSE_UNITS.get(code, "").replace("^3", "³")


def series_view(df) -> pn.Column:
    """One line chart per variable (columns are named like 'p.ERA5-Land')."""
    plots = []
    for slc, label in SLC_LABELS.items():
        cols = [c for c in df.columns if c.startswith(slc + ".")]
        if cols:
            plot = df[cols].hvplot.line(
                xlabel="Date",
                ylabel=f"{label} ({unit_of(slc)})",
                responsive=True,
                height=260,
            )
            # Mouse wheel scrolls the page (zoom is still in the toolbar), and
            # plain canvas drawing: WebGL plots next to the WebGL map can stay blank.
            plot = plot.opts(
                active_tools=["pan"], backend_opts={"plot.output_backend": "canvas"}
            )
            plots.append(plot)
    return pn.Column(*plots, sizing_mode="stretch_width", name="Time series")


def caveats_view(found: list[dict]) -> pn.pane.Markdown:
    """Known data issues for this lake, as a plain list."""
    lines = [f"Known data issues that apply to this lake: {len(found)}\n"]
    for c in found:
        lines.append(f"- **{c['level'].upper()}: {c['title']}** {c['message']}")
        if c.get("instruction"):
            lines.append(f"  *{c['instruction']}*")
        for ref in c.get("references", []):
            lines.append(f"  [{ref['title']}]({ref['url']})")
    return pn.pane.Markdown("\n".join(lines), name=f"Caveats ({len(found)})")


def table_view(df, name: str) -> pn.Column:
    """The monthly data as a table, plus a CSV download."""
    download = pn.widgets.FileDownload(
        callback=lambda: io.StringIO(df.to_csv()), filename=f"{name}.csv"
    )
    # On screen: months as "2000-01" and the unit in each column name.
    shown = df.rename(columns=lambda c: f"{c} ({unit_of(c)})")
    shown.index = shown.index.strftime("%Y-%m").rename("Month")
    table = pn.widgets.Tabulator(
        shown.reset_index(),
        pagination="remote",
        show_index=False,
        page_size=20,
        sizing_mode="stretch_width",
    )
    return pn.Column(download, table, sizing_mode="stretch_width", name="Data")


def describe(lake: dict) -> str:
    """'**Lake Titicaca** · Peru · 7,000 km² · #12'"""
    parts = [f"**{(lake.get('name') or 'Unnamed lake').replace('_', ' ')}**"]
    if lake.get("country"):
        parts.append(lake["country"])
    area = lake.get("area_km2")
    if area is not None:
        parts.append(f"{area:,.0f} km²" if area >= 10 else f"{area:.2g} km²")
    parts.append(f"#{lake['hylak_id']}")
    return " · ".join(parts)


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------
def shared_job() -> jobs.ClayJob:
    """One CLAY job for the whole server (CLAY runs one lake at a time)."""
    if "glhe_job" not in pn.state.cache:
        pn.state.cache["glhe_job"] = jobs.ClayJob(max_seconds=public.max_run_seconds())
    return pn.state.cache["glhe_job"]


def in_background(work, then) -> None:
    """Run ``work()`` in a thread so the page stays responsive, then
    ``then(result)`` back on the page. Outside a live page, run both now."""
    doc = pn.state.curdoc
    if doc is None or doc.session_context is None:
        then(work())
        return

    def run():
        try:
            result = work()
        except Exception:
            logger.exception("Background work failed")
            result = None

        def apply():
            with set_curdoc(doc):  # so Panel knows which page this is for
                then(result)

        doc.add_next_tick_callback(apply)

    threading.Thread(target=run, daemon=True).start()


# --------------------------------------------------------------------------
# The page
# --------------------------------------------------------------------------
class Lime:
    """One browser page: its widgets and what happens when they are used."""

    def __init__(self, output_dir=None, job=None, root=None):
        self.job = job or shared_job()
        self.root = root  # folder of processed lakes (None = CLAY's output folder)
        self.selected = None  # the lake picked in the sidebar (a dict)
        self.catalog = []  # processed lakes
        self.started_here = False  # did this page start the current CLAY run?
        self.last_status = self.job.status

        # Find a lake
        self.search = pn.widgets.TextInput(
            placeholder="Lake name, country or HYLAK_ID, then Enter"
        )
        self.search.param.watch(self.on_search, "enter_pressed")
        self.results = pn.widgets.Select(options={}, size=6, visible=False)
        self.results.param.watch(self.on_result_chosen, "value")
        self.lake_text = pn.pane.Markdown("Search, or click a lake on the globe.")

        # Run CLAY
        self.run_button = pn.widgets.Button(label="Run CLAY", color="primary")
        self.run_button.on_click(lambda _: self.run_clay())
        self.cancel_button = pn.widgets.Button(label="Cancel", visible=False)
        self.cancel_button.on_click(lambda _: self.cancel_clay())
        self.status_text = pn.pane.Markdown("")
        self.log = pn.widgets.TextAreaInput(disabled=True, height=220, visible=False)

        # Processed lakes
        self.processed = pn.widgets.Select(label="Processed lakes", options={})
        self.processed.param.watch(self.on_processed_chosen, "value")
        self.refresh_button = pn.widgets.Button(label="Refresh list")
        self.refresh_button.on_click(lambda _: self.refresh_catalog())

        # Main area
        self.globe = LakeGlobe(
            lake_tiles_url=lake_tiles_url(), height=450, sizing_mode="stretch_width"
        )
        self.globe.param.watch(self.on_globe_pick, "picked")
        self.layer = pn.widgets.Select(label="Gridded layer on globe", visible=False)
        self.layer.param.watch(self.on_layer_chosen, "value")
        self.layer_text = pn.pane.Markdown("")
        self.title = pn.pane.Markdown("")
        self.tabs = pn.Tabs(sizing_mode="stretch_width")
        self.lake = None  # the processed lake on show (from load_lake)

        self.refresh_catalog()
        if output_dir:
            self.load(output_dir)
        self.poll_job()
        if pn.state.curdoc is not None and pn.state.curdoc.session_context:
            pn.state.add_periodic_callback(self.poll_job, period=1000)

    def sidebar(self) -> list:
        return [
            "## Find a lake",
            self.search,
            self.results,
            self.lake_text,
            "## Run CLAY",
            pn.Row(self.run_button, self.cancel_button),
            self.status_text,
            self.log,
            "## Results",
            self.processed,
            self.refresh_button,
        ]

    def main(self) -> list:
        banner = [pn.pane.Alert(public.BANNER, alert_type="warning")]
        top = banner if public.enabled() else []
        return top + [self.globe, self.layer, self.layer_text, self.title, self.tabs]

    def template(self):
        return pn.template.BootstrapTemplate(
            title="GLHE: Global Lake Hydrology Explorer",
            sidebar=self.sidebar(),
            main=self.main(),
        )

    # ---- finding a lake ---------------------------------------------------
    def on_search(self, _event=None) -> None:
        text = self.search.value_input or self.search.value
        if not hydrolakes.available():
            self.lake_text.object = "HydroLAKES cannot be read, so search is off."
            return
        self.lake_text.object = "Searching..."
        in_background(lambda: hydrolakes.search(text, limit=8), self.show_results)

    def show_results(self, found) -> None:
        if not found:
            self.results.visible = False
            self.lake_text.object = "No lake found."
            return
        options = {describe(lake).replace("**", ""): lake for lake in found}
        self.results.param.update(options={"": None, **options}, visible=True)
        self.lake_text.object = f"{len(found)} found; choose one."

    def on_result_chosen(self, event) -> None:
        if event.new:
            self.select_lake(event.new["hylak_id"], event.new)

    def on_globe_pick(self, event) -> None:
        self.select_lake(event.new["hylak_id"], event.new)

    def select_lake(self, hylak_id: int, info: dict | None = None, fetch=True):
        """Make this lake the one to run; fetch and draw its outline unless
        ``fetch`` is False (a processed lake brings its own)."""
        self.selected = dict(info or {}, hylak_id=int(hylak_id))
        self.lake_text.object = describe(self.selected)
        self.update_run_button()
        if not fetch:
            return

        def outline():
            gdf = hydrolakes.get_lake(int(hylak_id)) if hydrolakes.available() else None
            return {} if gdf is None else json.loads(gdf.to_json())

        def draw(geojson):
            if self.selected and self.selected["hylak_id"] == int(hylak_id):
                self.globe.lake_geojson = geojson or {}

        in_background(outline, draw)

    # ---- running CLAY -----------------------------------------------------
    def processed_ids(self) -> set[int]:
        return {e["hylak_id"] for e in self.catalog if e["hylak_id"] is not None}

    def update_run_button(self) -> None:
        hylak_id = (self.selected or {}).get("hylak_id")
        blocked = None
        if public.enabled():
            blocked = public.lake_block_reason(self.selected, self.processed_ids())
        self.run_button.label = blocked or (
            f"Run CLAY for #{hylak_id}" if hylak_id else "Run CLAY"
        )
        self.run_button.disabled = (
            hylak_id is None or self.job.is_running or blocked is not None
        )
        # In public mode only the visitor who started a run may cancel it.
        self.cancel_button.visible = self.job.is_running and (
            self.started_here or not public.enabled()
        )

    def run_clay(self) -> None:
        if not self.selected:
            return
        visitor = public.visitor_id() if public.enabled() else None
        if visitor:
            reason = public.lake_block_reason(self.selected, self.processed_ids())
            reason = reason or public.visitor_block_reason(visitor)
            if reason:
                self.status_text.object = reason
                return
        if not self.job.start(self.selected["hylak_id"]):
            self.status_text.object = "CLAY is busy with another lake; try later."
            return
        if visitor:
            public.record_run(visitor)
        self.started_here = True
        self.poll_job()

    def cancel_clay(self) -> None:
        if self.started_here or not public.enabled():
            self.job.cancel()

    def poll_job(self) -> None:
        """Called every second: copy the job's status and log into the page."""
        job = self.job
        if job.hylak_id is not None:
            self.status_text.object = f"CLAY #{job.hylak_id}: {job.status}"
            self.log.visible = True
        self.log.value = "\n".join(job.log[-200:])
        self.update_run_button()

        just_finished = self.last_status == "running" and job.status != "running"
        self.last_status = job.status
        if not just_finished:
            return
        self.refresh_catalog()
        if self.started_here and job.status == "done" and job.output_dir:
            self.load(job.output_dir)
        self.started_here = False

    # ---- showing results --------------------------------------------------
    def refresh_catalog(self) -> None:
        self.catalog = catalog.list_processed_lakes(self.root)
        options = {"": None}
        for e in self.catalog:
            label = e["name"].replace("_", " ")
            options[f"{label} (#{e['hylak_id']})" if e["hylak_id"] else label] = e[
                "output_dir"
            ]
        self.processed.options = options
        self.update_run_button()

    def on_processed_chosen(self, event) -> None:
        if event.new:
            self.load(event.new)

    def on_layer_chosen(self, event) -> None:
        zip_path = self.lake and self.lake["gridded_zip"]
        cells = gridded.cells_geojson(zip_path, event.new) if event.new else {}
        self.globe.cells_geojson = cells
        values = [f["properties"]["value"] for f in cells.get("features", [])]
        self.layer_text.object = (
            f"Colours: purple = {min(values):.3g}, yellow = {max(values):.3g} "
            f"{unit_of(event.new)}"
            if values
            else ""
        )

    def load(self, folder) -> None:
        """Show a processed lake: outline on the globe and the result tabs."""
        try:
            lake = load_lake(folder)
        except Exception as exc:
            logger.exception("Could not open %s", folder)
            self.title.object = f"Could not open {folder}: {exc}"
            return
        self.lake = lake
        self.title.object = f"## {lake['name'].replace('_', ' ')}"
        if lake["hylak_id"] is not None:
            # The folder's own outline, else HydroLAKES' (fetched by select_lake).
            info = {"name": lake["name"]}
            self.select_lake(lake["hylak_id"], info, fetch=not lake["polygon"])
        if lake["polygon"]:
            self.globe.lake_geojson = lake["polygon"]
        self.globe.param.update(points_geojson=lake["points"], cells_geojson={})

        layers = gridded.list_layers(lake["gridded_zip"]) if lake["gridded_zip"] else []
        self.layer.param.update(options=["", *layers], value="", visible=bool(layers))

        df = lake["df"]
        if df is None:
            self.tabs.objects = [pn.pane.Markdown("No data file.", name="Data")]
        else:
            self.tabs.objects = [
                series_view(df),
                caveats_view(lake["caveats"]),
                table_view(df, lake["name"]),
            ]


def _url_arg(name: str) -> str | None:
    """?name=value from the page address, if any."""
    values = (pn.state.session_args or {}).get(name)
    return values[0].decode() if values else None


def build_app(output_dir=None, job=None, root=None):
    """The LIME page. ``template.lime`` is the Lime object behind it."""
    output_dir = (
        output_dir or _url_arg("output_dir") or os.environ.get("GLHE_OUTPUT_DIR")
    )
    lime = Lime(output_dir=output_dir, job=job, root=root)
    template = lime.template()
    template.lime = lime
    return template


def serve(output_dir=None, port: int = 5006, address: str | None = None, show=True):
    """Serve LIME from Python (same as ``panel serve GLHE/LIME/app.py``)."""
    return pn.serve(
        lambda: build_app(output_dir), port=port, address=address, show=show
    )


def warm_hydrolakes() -> None:
    """Load the HydroLAKES search index once per server, in the background."""
    if pn.state.cache.setdefault("glhe_warm", False):
        return
    pn.state.cache["glhe_warm"] = True
    if hydrolakes.available():
        threading.Thread(target=hydrolakes.lake_index, daemon=True).start()


# `panel serve GLHE/LIME/app.py` runs this file once per browser page.
if __name__.startswith("bokeh"):
    warm_hydrolakes()
    build_app().servable(title="GLHE")

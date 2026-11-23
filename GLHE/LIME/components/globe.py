"""
Interactive 3D globe for LIME (MapLibre GL JS), wrapped as a Panel component.

This is the only place LIME uses JavaScript. The JS lives next to this file in
``globe.js`` and is heavily commented; everything you normally need is
controlled from Python through the params below.

Example
-------
>>> globe = LakeGlobe(lake_geojson=state.polygon_geojson, dark=True)
>>> globe.cells_geojson = feature_collection_from_gdf(cells_gdf)   # updates live
>>> globe.param.watch(lambda e: print(e.new), "clicked")           # {"lon", "lat"}

All ``*_geojson`` params are GeoJSON FeatureCollections as plain dicts
(``{}`` means "draw nothing"), in WGS 1984 / EPSG:4326.

* ``lake_geojson``   lake polygon(s); the globe flies to it when it changes.
  While it is empty the globe slowly spins.
* ``points_geojson`` points such as the NWM lake point.
* ``cells_geojson``  gridded cells; each feature's ``properties["value"]`` is
  coloured on a sequential ramp (or set ``properties["color"]`` yourself).
* ``caveat_geojson`` caveat areas, drawn with a dashed outline whose colour
  follows ``properties["level"]`` ("warning" default, "error"/"danger",
  "info").

Run this file directly for a demo using the test fixture lake::

    python -m panel serve GLHE/LIME/components/globe.py --show
"""

from __future__ import annotations

import json
import pathlib

import panel as pn
import param

from GLHE.LIME import theme

#: MapLibre GL JS version. Keep it equal to the version in globe.js's import.
MAPLIBRE_VERSION = "5.6.0"
MAPLIBRE_CSS = (
    f"https://cdn.jsdelivr.net/npm/maplibre-gl@{MAPLIBRE_VERSION}/dist/maplibre-gl.css"
)

# CSS applied inside the component (Panel components live in a shadow DOM, so
# page-level CSS does not reach in here).
_GLOBE_CSS = """
:host { display: block; }
.glhe-globe {
  width: 100%;
  height: 100%;
  min-height: 300px;
  border-radius: 12px;
  overflow: hidden;
  background: radial-gradient(circle at 50% 45%, #13213d 0%, #05080f 75%);
}
.maplibregl-ctrl-attrib { font-size: 10px; }
"""


def globe_colors(dark: bool = True) -> dict:
    """Colours handed to the JS, taken from the LIME theme tokens."""
    c = theme.COLORS["dark" if dark else "light"]
    return {
        "bg": c["bg"],
        "accent": c["accent"],
        "warning": c["warning"],
        "danger": c["danger"],
        "info": theme.slc_color("p", dark),
        "point": "#ffffff" if dark else c["text"],
        # Sequential ramp for gridded cells (low -> high value).
        "ramp_low": "#0c2a4d" if dark else "#e0f2fe",
        "ramp_high": "#7ee8fa" if dark else "#0c4a6e",
    }


class LakeGlobe(pn.custom.JSComponent):
    """A MapLibre globe showing the lake, NWM point, gridded cells and caveats."""

    lake_geojson = param.Dict(default={}, doc="Lake polygon FeatureCollection")
    points_geojson = param.Dict(default={}, doc="Point FeatureCollection (NWM)")
    cells_geojson = param.Dict(
        default={}, doc="Gridded cells; colour by properties['value']"
    )
    caveat_geojson = param.Dict(default={}, doc="Caveat areas (dashed outline)")
    clicked = param.Dict(
        default={}, doc="Last map click as {'lon', 'lat'}; set from the browser"
    )
    dark = param.Boolean(default=True, doc="Dark basemap (True) or light")
    colors = param.Dict(default={}, doc="Colour tokens; filled from theme.py")

    _esm = pathlib.Path(__file__).with_name("globe.js")
    _stylesheets = [MAPLIBRE_CSS, _GLOBE_CSS]

    def __init__(self, **params):
        params.setdefault("height", 480)
        params.setdefault("sizing_mode", "stretch_width")
        if not params.get("colors"):
            params["colors"] = globe_colors(params.get("dark", True))
        super().__init__(**params)

    @param.depends("dark", watch=True)
    def _update_colors(self):
        """Switch colour tokens along with the basemap."""
        self.colors = globe_colors(self.dark)


# ---------------------------------------------------------------------------
# Small helpers for turning Python geometry into the dicts the globe expects
# ---------------------------------------------------------------------------


def feature_collection_from_gdf(gdf) -> dict:
    """GeoDataFrame -> GeoJSON FeatureCollection dict in EPSG:4326.

    Returns ``{}`` for ``None`` or an empty frame.
    """
    if gdf is None or len(gdf) == 0:
        return {}
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    return json.loads(gdf.to_json())


def feature_collection_from_shape(geom, **properties) -> dict:
    """Shapely geometry -> one-feature FeatureCollection dict (``{}`` if None)."""
    if geom is None or geom.is_empty:
        return {}
    from shapely.geometry import mapping

    return {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "geometry": mapping(geom), "properties": properties}
        ],
    }


def read_points(path) -> dict:
    """Read a point shapefile (e.g. the NWM lake point) into a FeatureCollection.

    Returns ``{}`` if the path is missing or unreadable.
    """
    if path is None or not pathlib.Path(path).exists():
        return {}
    try:
        import geopandas as gpd

        return feature_collection_from_gdf(gpd.read_file(path))
    except Exception:  # noqa: BLE001 - a missing point should never break the app
        return {}


def _demo() -> pn.viewable.Viewable:
    """Fixture lake on the globe, with fake cells and a caveat box."""
    from shapely.geometry import box, mapping

    from GLHE.LIME.state import AppState

    repo = pathlib.Path(__file__).resolve().parents[3]
    state = AppState()
    state.load_output_dir(repo / "tests" / "LIME" / "fixtures" / "Fixture_Lake")

    # A fake 10 x 10 grid of 0.1 degree cells over the fixture lake.
    cells = [
        {
            "type": "Feature",
            "geometry": mapping(box(lon, lat, lon + 0.1, lat + 0.1)),
            "properties": {"value": i + j},
        }
        for i, lon in enumerate(-112.9 + 0.1 * k for k in range(10))
        for j, lat in enumerate(40.7 + 0.1 * k for k in range(10))
    ]

    globe = LakeGlobe(
        lake_geojson=state.polygon_geojson,
        points_geojson=read_points(state.path_for("NWM_LAKE_POINT_SHAPEFILENAME")),
        cells_geojson={"type": "FeatureCollection", "features": cells},
        caveat_geojson=feature_collection_from_shape(
            box(-113.4, 40.4, -111.5, 42.0), level="warning", title="Demo caveat"
        ),
    )
    dark = pn.widgets.Toggle(name="Dark", value=True)
    dark.link(globe, value="dark")
    clear = pn.widgets.Button(name="Clear lake (spin)")
    clear.on_click(lambda _: globe.param.update(lake_geojson={}, cells_geojson={}))
    reset = pn.widgets.Button(name="Fly to lake")
    reset.on_click(lambda _: setattr(globe, "lake_geojson", state.polygon_geojson))
    clicked = pn.pane.JSON(globe.param.clicked, depth=1)
    return pn.Column(
        pn.Row(dark, clear, reset, clicked), globe, sizing_mode="stretch_width"
    )


if __name__.startswith("bokeh"):
    theme.apply_theme()
    _demo().servable(title="LakeGlobe demo")
elif __name__ == "__main__":
    theme.apply_theme()
    pn.serve(_demo, port=5091, show=True)

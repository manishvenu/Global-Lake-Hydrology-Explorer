"""
Gridded tab: view the GeoTIFF layers CLAY saves in its gridded-data zip.

CLAY writes ``{LAKE}_gridded_data_layers_on_{YYYYMMDD}.zip`` containing one
GeoTIFF per variable and product, named ``{slc}_{product}_{lake}_{date}.tif``
(for example ``p_ERA5-Land_Great_Salt_Lake_20000101.tif``).

The tifs are read straight out of the zip with rasterio's ``zip://`` path
syntax, so nothing is extracted to disk.

What the tab shows
------------------
* A layer selector (e.g. "Precipitation · ERA5-Land") and, when a layer has
  more than one date, a date slider.
* A local heat-map of the raster (hvplot/HoloViews ``Image`` with colour bar)
  with the lake outline on top.
* If a ``LakeGlobe`` is passed in, the same cells are pushed to
  ``globe.cells_geojson`` as GeoJSON polygons. Each feature has properties
  ``value`` (float), ``color`` (hex string, already mapped through the colour
  map, so the globe can paint it directly), ``slc``, ``product`` and ``date``.
"""

from __future__ import annotations

import html

import zipfile
from pathlib import Path

import holoviews as hv
import numpy as np
import pandas as pd
import panel as pn
from bokeh.palettes import Viridis256

from GLHE.LIME import theme
from GLHE.LIME.components.common import NO_DATA_MESSAGE, empty_state, is_dark

#: Colour map (list of hex colours) used for every gridded layer.
CMAP = list(Viridis256)

#: Safety limit: very large rasters are not sent to the browser globe.
MAX_GLOBE_CELLS = 20_000

# ---------------------------------------------------------------------------
# Reading the zip
# ---------------------------------------------------------------------------


def parse_layer_name(member: str) -> dict | None:
    """
    Split a tif name like 'p_ERA5-Land_Great_Salt_Lake_20000101.tif'.

    Returns a dict with ``member``, ``slc``, ``product``, ``date``
    (pd.Timestamp) and ``key`` ('p.ERA5-Land', the same naming as the CSV
    columns), or None if the name does not follow CLAY's pattern.
    """
    name = Path(member).name
    if not name.lower().endswith((".tif", ".tiff")):
        return None
    parts = Path(name).stem.split("_")
    if len(parts) < 3:
        return None
    slc, product, date_text = parts[0], parts[1], parts[-1]
    date = pd.to_datetime(date_text, format="%Y%m%d", errors="coerce")
    if pd.isna(date):
        return None
    return {
        "member": member,
        "slc": slc,
        "product": product,
        "date": date,
        "key": f"{slc}.{product}",
    }


def list_layers(zip_path: str | Path) -> list[dict]:
    """All recognised GeoTIFF layers in a CLAY gridded zip, sorted by key/date."""
    with zipfile.ZipFile(zip_path) as zf:
        layers = [parse_layer_name(m) for m in zf.namelist()]
    layers = [layer for layer in layers if layer is not None]
    return sorted(layers, key=lambda layer: (layer["key"], layer["date"]))


def read_layer(zip_path: str | Path, member: str):
    """
    Read band 1 of one tif inside the zip.

    Returns ``(data, transform, bounds)`` where ``data`` is a float array with
    nodata replaced by NaN, ``transform`` is the rasterio Affine and
    ``bounds`` is ``(left, bottom, right, top)`` in degrees.
    """
    import rasterio  # imported lazily; only this tab needs it

    path = f"zip://{Path(zip_path).resolve()}!/{member}"
    with rasterio.open(path) as src:
        data = src.read(1).astype(float)
        if src.nodata is not None:
            data[data == src.nodata] = np.nan
        return data, src.transform, tuple(src.bounds)


# ---------------------------------------------------------------------------
# Converting for display
# ---------------------------------------------------------------------------


def value_to_color(values: np.ndarray, vmin: float, vmax: float) -> list[str]:
    """Map values onto ``CMAP`` (linear between vmin and vmax)."""
    span = (vmax - vmin) or 1.0
    idx = np.clip(((values - vmin) / span * (len(CMAP) - 1)).round(), 0, len(CMAP) - 1)
    return [CMAP[int(i)] for i in idx]


def cells_geojson(data: np.ndarray, transform, properties: dict | None = None) -> dict:
    """
    Turn a raster into a GeoJSON FeatureCollection of cell rectangles.

    Only finite cells are included. Each feature gets ``value`` and ``color``
    plus anything in ``properties`` (e.g. slc/product/date). Returns ``{}``
    when there is nothing to draw or the raster is too large for the globe.
    """
    rows, cols = np.nonzero(np.isfinite(data))
    if len(rows) == 0 or len(rows) > MAX_GLOBE_CELLS:
        return {}
    values = data[rows, cols]
    colors = value_to_color(values, float(np.nanmin(values)), float(np.nanmax(values)))
    extra = properties or {}
    # Upper-left and lower-right corners of every cell, from the affine transform.
    a, b, x_off, d, e, y_off = transform[:6]
    x0s, y0s = x_off + a * cols + b * rows, y_off + d * cols + e * rows
    x1s, y1s = x0s + a + b, y0s + d + e
    features = []
    for x0, y0, x1, y1, v, color in zip(x0s, y0s, x1s, y1s, values, colors):
        x0, y0, x1, y1 = float(x0), float(y0), float(x1), float(y1)
        ring = [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [ring]},
                "properties": {"value": float(v), "color": color, **extra},
            }
        )
    return {"type": "FeatureCollection", "features": features}


def lake_outline(polygon_geojson: dict) -> hv.Path | None:
    """Lake polygon boundary as a HoloViews Path (None if no polygon)."""
    features = (polygon_geojson or {}).get("features") or []
    if not features:
        return None
    from shapely.geometry import shape

    paths = []
    for f in features:
        geom = shape(f["geometry"])
        polys = getattr(geom, "geoms", [geom])
        for poly in polys:
            if hasattr(poly, "exterior"):
                paths.append(np.asarray(poly.exterior.coords))
    return hv.Path(paths, ["Longitude", "Latitude"]) if paths else None


def raster_plot(data, bounds, title: str, unit: str, polygon_geojson: dict, dark: bool):
    """Heat-map of one raster with a colour bar and the lake outline."""
    left, bottom, right, top = bounds
    # hv.Image expects row 0 at the top, which is how GeoTIFFs are stored.
    image = hv.Image(
        data,
        bounds=(left, bottom, right, top),
        kdims=["Longitude", "Latitude"],
        vdims=[hv.Dimension("value", label=unit)],
    )
    # Keep degrees roughly square on screen at this latitude.
    mid_lat = np.deg2rad((top + bottom) / 2)
    aspect = max(0.2, min(5.0, (right - left) * np.cos(mid_lat) / (top - bottom)))
    c = theme.COLORS["dark" if dark else "light"]

    def _style_colorbar(plot, element):
        """Make the colour bar match the theme (transparent, muted labels)."""
        for bar in plot.state.right:
            if hasattr(bar, "major_label_text_color"):
                bar.background_fill_alpha = 0
                bar.major_label_text_color = c["text_muted"]
                bar.title_text_color = c["text_muted"]

    image = image.opts(
        cmap=CMAP,
        colorbar=True,
        clabel=unit,
        tools=["hover"],
        title=title,
        frame_height=380,
        aspect=aspect,
        hooks=theme.chart_opts(dark)["hooks"] + [_style_colorbar],
    )
    outline = lake_outline(polygon_geojson)
    if outline is None:
        return image
    return image * outline.opts(color=c["text"], line_width=2)


# ---------------------------------------------------------------------------
# View
# ---------------------------------------------------------------------------


def _layer_label(key: str) -> str:
    """'p.ERA5-Land' -> 'Precipitation · ERA5-Land'."""
    slc, _, product = key.partition(".")
    return f"{theme.SLC_LABELS.get(slc, slc)} · {product}"


def _build(zip_path: Path | None, polygon_geojson: dict, globe=None):
    """Selectors + raster view for one gridded zip (rebuilt when the lake changes)."""
    if globe is not None:
        globe.cells_geojson = {}  # clear cells from any previous lake
    if zip_path is None:
        return empty_state(
            "No gridded data for this lake. CLAY writes it as "
            "<code>*_gridded_data_layers_on_*.zip</code>."
        )
    try:
        layers = list_layers(zip_path)
    except (OSError, zipfile.BadZipFile) as err:
        return empty_state(f"Could not open the gridded zip: {err}")
    if not layers:
        return empty_state("The gridded zip contains no recognised GeoTIFFs.")

    dark = is_dark()
    by_key: dict[str, list[dict]] = {}
    for layer in layers:
        by_key.setdefault(layer["key"], []).append(layer)

    layer_select = pn.widgets.Select(
        label="Layer",
        options={_layer_label(k): k for k in by_key},
        width=260,
        sizing_mode=None,
    )
    date_slider = pn.widgets.DiscreteSlider(label="Date", width=320, sizing_mode=None)
    show_on_globe = pn.widgets.Switch(value=True, width=40, visible=globe is not None)

    def _update_dates(*_):
        """Fill the date slider with the dates available for the chosen layer."""
        options = {
            f"{lyr['date']:%Y-%m}": lyr["member"] for lyr in by_key[layer_select.value]
        }
        date_slider.options = options
        date_slider.value = next(iter(options.values()))
        # A slider with a single stop is just noise; hide it.
        date_slider.visible = len(options) > 1

    layer_select.param.watch(_update_dates, "value")
    _update_dates()

    by_member = {layer["member"]: layer for layer in layers}

    def _render(key: str, member: str, on_globe: bool):
        """Read the chosen tif, draw it locally and (optionally) on the globe."""
        # Look the layer up by member (not key): the slider is the source of
        # truth for which tif is shown, even mid-way through a layer change.
        info = by_member[member]
        key = info["key"]
        try:
            data, transform, bounds = read_layer(zip_path, member)
        except Exception as err:  # corrupt/unsupported tif inside the zip
            if globe is not None:
                globe.cells_geojson = {}
            return empty_state(
                f"Could not read {html.escape(member)}: {html.escape(str(err))}"
            )
        if globe is not None:
            props = {
                "slc": info["slc"],
                "product": info["product"],
                "date": f"{info['date']:%Y-%m-%d}",
            }
            globe.cells_geojson = (
                cells_geojson(data, transform, props) if on_globe else {}
            )
        title = f"{_layer_label(key)} · {info['date']:%b %Y}"
        unit = theme.SLC_UNITS.get(info["slc"], "")
        finite = data[np.isfinite(data)]
        stats = (
            f"{finite.size} cells · min {finite.min():.3g} · "
            f"mean {finite.mean():.3g} · max {finite.max():.3g} {unit}"
            if finite.size
            else "All cells are empty (nodata)."
        )
        return pn.Column(
            pn.pane.HoloViews(
                raster_plot(data, bounds, title, unit, polygon_geojson, dark),
                linked_axes=False,
                sizing_mode=None,  # size comes from the plot's frame_height/aspect
            ),
            pn.pane.HTML(f'<div class="glhe-muted">{stats}</div>'),
            sizing_mode="stretch_width",
        )

    # FlexBox (not Row) so the controls wrap on narrow screens.
    controls = pn.FlexBox(
        layer_select,
        date_slider,
        pn.Row(
            show_on_globe,
            pn.pane.HTML(
                '<span class="glhe-kpi-label">Show cells on globe</span>',
                sizing_mode=None,
            ),
            visible=globe is not None,
            margin=(22, 10, 0, 10),
            sizing_mode=None,
        ),
        sizing_mode="stretch_width",
    )
    view = pn.bind(_render, layer_select, date_slider, show_on_globe)
    return pn.Column(controls, pn.panel(view), sizing_mode="stretch_width")


def make_gridded(state, globe=None) -> pn.viewable.Viewable:
    """
    Gridded tab for an ``AppState``.

    ``globe`` is an optional ``LakeGlobe``; when given, the selected layer's
    cells are written to ``globe.cells_geojson``. Rebuilds when the state's
    output folder / config or polygon change.
    """

    def _for_state(output_dir, config, polygon_geojson):
        if not output_dir:
            if globe is not None:
                globe.cells_geojson = {}
            return empty_state(NO_DATA_MESSAGE)
        return _build(state.path_for("GRIDDED_DATA_FOLDER"), polygon_geojson, globe)

    return pn.panel(
        pn.bind(
            _for_state,
            state.param.output_dir,
            state.param.config,
            state.param.polygon_geojson,
        ),
        sizing_mode="stretch_width",
    )

// ---------------------------------------------------------------------------
// LakeGlobe: the MapLibre GL globe used by LIME (the only JavaScript in LIME).
//
// Python side: GLHE/LIME/components/globe.py (class LakeGlobe).
// Panel calls `render({model, el})` once per view. `model` holds the Python
// params (model.lake_geojson, model.dark, ...). Reading them gives the current
// value, `model.on("name", fn)` runs fn when Python changes one, and assigning
// (model.clicked = {...}) sends a value back to Python.
//
// Map layers, drawn bottom to top:
//   lakes   - every HydroLAKES lake, from vector tiles (model.lake_tiles_url);
//             hover shows a tooltip, click sends model.picked to Python
//   cells   - gridded data cells, filled by their "value" property (or "color")
//   caveat  - caveat areas, dashed outline coloured by their "level"
//   lake    - the lake polygon: accent fill plus a soft glow outline
//   points  - NWM lake point(s)
// ---------------------------------------------------------------------------

// Pinned versions. Keep them in step with MAPLIBRE_VERSION / PMTILES_VERSION
// in globe.py.
import maplibregl from "https://esm.sh/maplibre-gl@5.6.0";
import { Protocol } from "https://esm.sh/pmtiles@4.3.0";

// PMTiles is one big file of map tiles that the browser reads piece by piece
// (HTTP Range requests), so no tile server is needed. Registering the
// protocol lets sources use URLs like "pmtiles://https://.../lakes.pmtiles".
// It is global to the page, so only register it once.
if (!window.__glhePmtiles) {
  maplibregl.addProtocol("pmtiles", new Protocol().tile);
  window.__glhePmtiles = true;
}

// Free basemaps that need no API token (CARTO vector styles).
const BASEMAPS = {
  dark: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
  light: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
};

const EMPTY = { type: "FeatureCollection", features: [] };
const SOURCES = ["cells", "caveat", "lake", "points"];

// Bokeh can hand us dicts as JS Map objects; turn them into plain objects.
function plain(x) {
  if (x instanceof Map) {
    const out = {};
    x.forEach((v, k) => (out[k] = plain(v)));
    return out;
  }
  if (Array.isArray(x)) return x.map(plain);
  return x;
}

// Python sends {} for "nothing"; MapLibre needs a valid FeatureCollection.
function asFC(value) {
  const v = plain(value);
  if (!v || !v.type) return EMPTY;
  if (v.type === "FeatureCollection") return v;
  if (v.type === "Feature") return { type: "FeatureCollection", features: [v] };
  return { type: "FeatureCollection", features: [{ type: "Feature", geometry: v, properties: {} }] };
}

// Bounding box [[west, south], [east, north]] of every coordinate in a FeatureCollection.
function bbox(fc) {
  let b = [Infinity, Infinity, -Infinity, -Infinity];
  const walk = (c) => {
    if (typeof c[0] === "number") {
      b = [Math.min(b[0], c[0]), Math.min(b[1], c[1]), Math.max(b[2], c[0]), Math.max(b[3], c[1])];
    } else c.forEach(walk);
  };
  fc.features.forEach((f) => f.geometry && walk(f.geometry.coordinates));
  return isFinite(b[0]) ? [[b[0], b[1]], [b[2], b[3]]] : null;
}

// Min/max of the "value" property, for the cell colour ramp.
function valueRange(fc) {
  const vals = fc.features.map((f) => Number(f.properties && f.properties.value)).filter(isFinite);
  if (!vals.length) return [0, 1];
  const lo = Math.min(...vals), hi = Math.max(...vals);
  return lo === hi ? [lo, lo + 1] : [lo, hi];
}

// Small HTML escape for the tooltip (lake names come from data).
function esc(s) {
  return String(s).replace(/[&<>"']/g, (ch) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch]);
}

// "6,479 km²" for big lakes, "0.13 km²" for small ones.
function areaText(a) {
  const n = Number(a);
  if (!isFinite(n) || n <= 0) return "";
  return (n >= 10 ? Math.round(n).toLocaleString() : n.toPrecision(2)) + " km²";
}

// The lake tiles URL as pmtiles://<absolute URL> ("/tiles/x.pmtiles" works too).
function tilesSource(url) {
  if (!url) return null;
  return "pmtiles://" + new URL(url, window.location.href).href;
}

export function render({ model, el }) {
  // The map needs a sized <div> to draw into.
  const container = document.createElement("div");
  container.className = "glhe-globe";
  el.appendChild(container);

  const colors = () => plain(model.colors) || {};
  const theme = () => (model.dark ? "dark" : "light");

  const map = new maplibregl.Map({
    container,
    style: BASEMAPS[theme()],
    center: [-40, 25],
    zoom: 1.4,
    attributionControl: { compact: true },
  });
  map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), "top-right");

  // ---- overlay layers ------------------------------------------------------
  // setStyle() (used when `dark` flips) wipes our layers, so everything is
  // (re)added in this function, which runs on every "style.load".
  function addOverlays() {
    const c = colors();
    map.setProjection({ type: "globe" });
    // Atmosphere glow around the globe, fading out as you zoom in.
    map.setSky({
      "sky-color": c.bg || "#0b1220",
      "horizon-color": c.accent || "#2dd4bf",
      "fog-color": c.bg || "#0b1220",
      "atmosphere-blend": ["interpolate", ["linear"], ["zoom"], 0, 1, 5, 1, 7, 0],
    });
    if (map.getLayer("lake-fill")) return; // already added for this style
    addLakeTiles(c);
    for (const id of SOURCES) {
      if (!map.getSource(id)) map.addSource(id, { type: "geojson", data: EMPTY });
    }
    const accent = c.accent || "#2dd4bf";
    const level = (fallback) => ["match", ["get", "level"],
      ["error", "danger", "critical"], c.danger || "#f26d6d",
      ["info", "note"], c.info || "#4ea8ff",
      fallback];

    map.addLayer({ id: "cells-fill", type: "fill", source: "cells",
      paint: { "fill-color": "#888", "fill-opacity": 0.6, "fill-outline-color": "rgba(0,0,0,0)" } });
    map.addLayer({ id: "caveat-fill", type: "fill", source: "caveat",
      paint: { "fill-color": level(c.warning || "#f5b94a"), "fill-opacity": 0.08 } });
    map.addLayer({ id: "caveat-line", type: "line", source: "caveat",
      paint: { "line-color": level(c.warning || "#f5b94a"), "line-width": 1.6, "line-dasharray": [3, 2] } });
    map.addLayer({ id: "lake-fill", type: "fill", source: "lake",
      paint: { "fill-color": accent, "fill-opacity": 0.28 } });
    // Glow = a wide blurred line under a thin sharp one.
    map.addLayer({ id: "lake-glow", type: "line", source: "lake",
      paint: { "line-color": accent, "line-width": 10, "line-blur": 8, "line-opacity": 0.55 } });
    map.addLayer({ id: "lake-line", type: "line", source: "lake",
      paint: { "line-color": accent, "line-width": 1.8 } });
    map.addLayer({ id: "points", type: "circle", source: "points",
      paint: { "circle-radius": 5, "circle-color": c.point || "#ffffff",
               "circle-stroke-color": accent, "circle-stroke-width": 2 } });
    for (const id of SOURCES) setData(id);
  }

  // ---- every lake, from vector tiles --------------------------------------
  // The tiles hold one polygon per lake (layer "lakes") with the properties
  // Hylak_id, Lake_name, Country and Lake_area. "promoteId" makes Hylak_id the
  // feature id, which hover highlighting (feature-state) needs. Small lakes
  // appear as you zoom in.
  const TILE_LAYERS = ["lakes-fill", "lakes-line"];
  function addLakeTiles(c) {
    hoverId = null; // a new style has no hover state
    const url = tilesSource(model.lake_tiles_url);
    if (!url || map.getSource("lakes")) return;
    map.addSource("lakes", { type: "vector", url, promoteId: "Hylak_id" });
    const accent = c.accent || "#2dd4bf";
    const hovered = ["boolean", ["feature-state", "hover"], false];
    map.addLayer({ id: "lakes-fill", type: "fill", source: "lakes", "source-layer": "lakes",
      paint: { "fill-color": accent, "fill-opacity": ["case", hovered, 0.45, 0.12] } });
    map.addLayer({ id: "lakes-line", type: "line", source: "lakes", "source-layer": "lakes",
      paint: { "line-color": accent, "line-opacity": ["case", hovered, 0.95, 0.35],
               "line-width": ["case", hovered, 1.6, 0.5] } });
  }
  const hasTiles = () => TILE_LAYERS.every((id) => map.getLayer(id));

  // Hover: highlight the lake under the mouse and show a small tooltip.
  const tip = new maplibregl.Popup({ closeButton: false, closeOnClick: false,
    className: "glhe-tip", offset: 12, maxWidth: "260px" });
  let hoverId = null;
  function setHover(id) {
    if (hoverId !== null && map.getSource("lakes")) {
      map.setFeatureState({ source: "lakes", sourceLayer: "lakes", id: hoverId }, { hover: false });
    }
    hoverId = id == null ? null : id;
    if (hoverId !== null) {
      map.setFeatureState({ source: "lakes", sourceLayer: "lakes", id }, { hover: true });
    }
  }
  map.on("mousemove", "lakes-fill", (e) => {
    const f = e.features && e.features[0];
    if (!f) return;
    map.getCanvas().style.cursor = "pointer";
    if ((f.id ?? null) !== hoverId) setHover(f.id ?? null);
    const p = f.properties || {};
    const c = colors();
    const area = areaText(p.Lake_area);
    tip.setLngLat(e.lngLat).setHTML(
      `<div style="color:${c.text};"><b>${esc(p.Lake_name || "Unnamed lake")}</b>` +
      `<div style="color:${c.muted};">${[p.Country, area, "#" + (p.Hylak_id ?? f.id)]
        .filter(Boolean).map(esc).join(" · ")}</div></div>`
    ).addTo(map);
    const box = tip.getElement() && tip.getElement().querySelector(".maplibregl-popup-content");
    if (box) box.style.background = c.surface || "#111a2e";
  });
  map.on("mouseleave", "lakes-fill", () => {
    map.getCanvas().style.cursor = "";
    setHover(null);
    tip.remove();
  });

  // Push the current Python value of `<id>_geojson` into map source `id`.
  function setData(id) {
    const src = map.getSource(id);
    if (!src) return; // style still loading; addOverlays() will catch up
    const fc = asFC(model[id + "_geojson"]);
    src.setData(fc);
    if (id === "cells") {
      // Sequential ramp from the low to the high colour across the data range;
      // a feature's own "color" property wins if present.
      const [lo, hi] = valueRange(fc);
      const c = colors();
      map.setPaintProperty("cells-fill", "fill-color", ["coalesce", ["get", "color"],
        ["interpolate", ["linear"], ["to-number", ["get", "value"], lo],
          lo, c.ramp_low || "#0c2a4d", hi, c.ramp_high || "#7ee8fa"]]);
    }
  }

  // Fly (smoothly zoom) to the lake outline.
  function flyToLake() {
    const b = bbox(asFC(model.lake_geojson));
    if (!b) return;
    stopSpin();
    // Keep the padding smaller than the map (phones, or a map that has not
    // been laid out yet), otherwise MapLibre cannot fit the bounds.
    const el = map.getContainer();
    const pad = Math.max(0, Math.min(60, el.clientWidth / 6, el.clientHeight / 6));
    const camera = map.cameraForBounds(b, { padding: pad, maxZoom: 10 });
    if (!camera) {
      // Still no room: just centre on the lake.
      map.jumpTo({ center: [(b[0][0] + b[1][0]) / 2, (b[0][1] + b[1][1]) / 2] });
      return;
    }
    map.flyTo({ ...camera, duration: 2800, essential: true });
  }

  // ---- slow auto-rotate while no lake is selected ---------------------------
  let spinning = false;
  function spin() {
    if (!spinning || map.getZoom() > 3) return;
    const center = map.getCenter();
    center.lng -= 6; // degrees per step
    map.easeTo({ center, duration: 1000, easing: (t) => t });
  }
  function startSpin() {
    if (spinning) return;
    spinning = true;
    // Zoomed in (e.g. a lake was just cleared)? Pull back out to the whole
    // globe first; the "moveend" at the end of that flight starts the spin.
    if (map.getZoom() > 3) map.flyTo({ zoom: 1.4, pitch: 0, duration: 2000 });
    else spin();
  }
  function stopSpin() { spinning = false; map.stop(); }
  map.on("moveend", spin); // each finished step schedules the next
  map.on("mousedown", stopSpin);
  map.on("touchstart", stopSpin);
  map.on("wheel", stopSpin);

  // ---- events: map -> Python ----------------------------------------------
  // A click on a lake drawn from the tiles sends that lake straight to Python
  // (model.picked); anywhere else sends the position (model.clicked), and
  // Python looks for a lake near it. "t" makes clicking the same lake twice
  // still count as a change.
  map.on("click", (e) => {
    const hit = hasTiles() ? map.queryRenderedFeatures(e.point, { layers: ["lakes-fill"] }) : [];
    if (hit.length) {
      const p = hit[0].properties || {};
      model.picked = { hylak_id: Number(p.Hylak_id ?? hit[0].id), name: p.Lake_name || null,
        country: p.Country || null, area_km2: p.Lake_area == null ? null : Number(p.Lake_area),
        t: Date.now() };
      return;
    }
    model.clicked = { lon: e.lngLat.lng, lat: e.lngLat.lat };
  });

  map.on("style.load", addOverlays);
  map.once("load", () => {
    if (asFC(model.lake_geojson).features.length) flyToLake();
    else startSpin();
  });

  // ---- events: Python -> map ----------------------------------------------
  model.on("lake_geojson", () => {
    setData("lake");
    if (asFC(model.lake_geojson).features.length) flyToLake();
    else startSpin();
  });
  model.on("points_geojson", () => setData("points"));
  model.on("cells_geojson", () => setData("cells"));
  model.on("caveat_geojson", () => setData("caveat"));
  model.on("dark", () => map.setStyle(BASEMAPS[theme()], { diff: false }));
  model.on("colors", () => map.setStyle(BASEMAPS[theme()], { diff: false }));
  model.on("lake_tiles_url", () => map.setStyle(BASEMAPS[theme()], { diff: false }));

  // Keep the canvas sized to the Panel layout; free GPU memory on removal.
  model.on("after_layout", () => map.resize());
  model.on("resize", () => map.resize());
  model.on("remove", () => map.remove());
}

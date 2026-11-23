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
//   cells   - gridded data cells, filled by their "value" property (or "color")
//   caveat  - caveat areas, dashed outline coloured by their "level"
//   lake    - the lake polygon: accent fill plus a soft glow outline
//   points  - NWM lake point(s)
// ---------------------------------------------------------------------------

// Pinned version. Keep it in step with MAPLIBRE_VERSION in globe.py (the CSS).
import maplibregl from "https://esm.sh/maplibre-gl@5.6.0";

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
  map.on("click", (e) => {
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

  // Keep the canvas sized to the Panel layout; free GPU memory on removal.
  model.on("after_layout", () => map.resize());
  model.on("resize", () => map.resize());
  model.on("remove", () => map.remove());
}

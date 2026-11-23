// The LIME globe (Python side: globe.py, class LakeGlobe).
//
// Panel calls render({model, el}) once. `model` holds the Python params:
// reading model.lake_geojson gives its value, model.on("lake_geojson", fn)
// runs fn when Python changes it, and assigning model.picked = {...} sends a
// value back to Python.

import maplibregl from "https://esm.sh/maplibre-gl@5.6.0";
import { Protocol } from "https://esm.sh/pmtiles@4.3.0";

// PMTiles: the browser reads map tiles straight out of one big file using
// HTTP Range requests, so no tile server is needed. Register it once per page.
if (!window.__pmtiles) {
  maplibregl.addProtocol("pmtiles", new Protocol().tile);
  window.__pmtiles = true;
}

const BLUE = "#1f77b4";
const EMPTY = { type: "FeatureCollection", features: [] };

// Python sends {} for "nothing"; Bokeh may send dicts as JS Maps.
function asFC(value) {
  const v = JSON.parse(JSON.stringify(value instanceof Map ? Object.fromEntries(value) : value || {}));
  return v.type === "FeatureCollection" ? v : EMPTY;
}

// [[west, south], [east, north]] around every coordinate, or null.
function bbox(fc) {
  let b = [180, 90, -180, -90];
  const walk = (c) => typeof c[0] === "number"
    ? (b = [Math.min(b[0], c[0]), Math.min(b[1], c[1]), Math.max(b[2], c[0]), Math.max(b[3], c[1])])
    : c.forEach(walk);
  fc.features.forEach((f) => f.geometry && walk(f.geometry.coordinates));
  return b[0] <= b[2] ? [[b[0], b[1]], [b[2], b[3]]] : null;
}

export function render({ model, el }) {
  const div = document.createElement("div");
  div.className = "map";
  el.appendChild(div);

  const map = new maplibregl.Map({
    container: div,
    style: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
    center: [0, 20],
    zoom: 1.3,
  });
  map.addControl(new maplibregl.NavigationControl(), "top-right");

  map.on("style.load", () => {
    map.setProjection({ type: "globe" });

    // Every lake, from the tiles (layer "lakes"). promoteId makes Hylak_id
    // the feature id, which the hover highlight needs.
    if (model.lake_tiles_url) {
      const url = "pmtiles://" + new URL(model.lake_tiles_url, window.location.href).href;
      map.addSource("lakes", { type: "vector", url, promoteId: "Hylak_id" });
      const hover = ["boolean", ["feature-state", "hover"], false];
      map.addLayer({ id: "lakes-fill", type: "fill", source: "lakes", "source-layer": "lakes",
        paint: { "fill-color": BLUE, "fill-opacity": ["case", hover, 0.5, 0.15] } });
      map.addLayer({ id: "lakes-line", type: "line", source: "lakes", "source-layer": "lakes",
        paint: { "line-color": BLUE, "line-width": 0.5 } });
    }

    // Our own layers: gridded cells, the selected lake, points.
    for (const id of ["cells", "lake", "points"]) {
      map.addSource(id, { type: "geojson", data: asFC(model[id + "_geojson"]) });
    }
    map.addLayer({ id: "cells", type: "fill", source: "cells",
      paint: { "fill-color": ["get", "color"], "fill-opacity": 0.7 } });
    map.addLayer({ id: "lake", type: "line", source: "lake",
      paint: { "line-color": "#d62728", "line-width": 2 } });
    map.addLayer({ id: "points", type: "circle", source: "points",
      paint: { "circle-radius": 5, "circle-color": "#000000" } });
    flyToLake();
  });

  // Hover over a tile lake: highlight it and show name, area and ID.
  const tip = new maplibregl.Popup({ closeButton: false, closeOnClick: false });
  let hoverId = null;
  function setHover(id) {
    if (hoverId !== null) map.setFeatureState({ source: "lakes", sourceLayer: "lakes", id: hoverId }, { hover: false });
    hoverId = id;
    if (id !== null) map.setFeatureState({ source: "lakes", sourceLayer: "lakes", id }, { hover: true });
  }
  map.on("mousemove", "lakes-fill", (e) => {
    const f = e.features[0];
    const p = f.properties;
    map.getCanvas().style.cursor = "pointer";
    if ((f.id ?? null) !== hoverId) setHover(f.id ?? null);
    const area = Number(p.Lake_area).toPrecision(3);
    tip.setLngLat(e.lngLat).addTo(map);
    tip.getElement().querySelector(".maplibregl-popup-content").textContent =
      `${p.Lake_name || "Unnamed lake"} · ${area} km² · #${p.Hylak_id}`;
  });
  map.on("mouseleave", "lakes-fill", () => {
    map.getCanvas().style.cursor = "";
    setHover(null);
    tip.remove();
  });

  // Click a tile lake -> send it to Python as model.picked.
  // "t" makes clicking the same lake twice still count as a change.
  map.on("click", "lakes-fill", (e) => {
    const p = e.features[0].properties;
    model.picked = { hylak_id: Number(p.Hylak_id), name: p.Lake_name || null,
      country: p.Country || null, area_km2: p.Lake_area == null ? null : Number(p.Lake_area), t: Date.now() };
  });

  function flyToLake() {
    const b = bbox(asFC(model.lake_geojson));
    if (b) map.fitBounds(b, { padding: 40, maxZoom: 10, duration: 2000 });
  }

  // Python changed a param: update that layer.
  const update = (id) => map.getSource(id) && map.getSource(id).setData(asFC(model[id + "_geojson"]));
  model.on("lake_geojson", () => { update("lake"); flyToLake(); });
  model.on("cells_geojson", () => update("cells"));
  model.on("points_geojson", () => update("points"));
  model.on("after_layout", () => map.resize());
  model.on("resize", () => map.resize());
  model.on("remove", () => map.remove());
}

# CLAUDE.md — Global Lake Hydrology Explorer (GLHE)

## What this project is

GLHE is a Python package for fetching, processing, and visualizing lake-scale hydrology data (precipitation, evapotranspiration, runoff) for any lake in the world, identified by its HydroLAKES ID (HYLAK_ID). It is structured as three subpackages:

- **CLAY** — data access and processing (the core). Downloads and spatially averages global datasets over a lake polygon.
- **LIME** — Panel web app: pick a lake, run CLAY, and explore the results (globe, charts, gridded data, caveats).
- **CALCITE** — internal pub/sub event bus that decouples CLAY and LIME.

The project is prototype-stage research software. It has not been scientifically validated.

---

## Environment setup

Conda is the recommended approach (Python 3.12):

```bash
conda env create -f environment.yml
conda activate glhe
```

The `environment.yml` at the repo root installs all conda and pip dependencies, then runs `pip install -e .[dev]` automatically — no separate pip step needed.

---

## Running the code

```python
import GLHE

# Lake ID lookup: https://glhe-fe.projects.earthengine.app/view/globallakehydrologyexplorer
HYLAK_ID = 67  # Great Salt Lake

clay = GLHE.CLAY.CLAY_driver.CLAY_driver()
output_dir = clay.main(HYLAK_ID)

# Optional: launch the web app on this lake
# GLHE.LIME.app.serve(output_dir)
```

**Web app (LIME)**:

```bash
panel serve GLHE/LIME/app.py --show                       # local
# open a lake directly: http://localhost:5006/app?output_dir=/path/to/lake/folder
# or: GLHE_OUTPUT_DIR=/path/to/lake/folder panel serve GLHE/LIME/app.py
# on the server (rootless Podman + Cloudflare Tunnel): see deploy/README.md
```

See `sample_script.py` for the canonical example. The first run downloads data; subsequent runs use local cache (pickle files in the output directory).

**First-run data**: The code uses AWS S3 by default. Call `GLHE.CLAY.data_access.data_check.download_for_offline()` once to cache everything locally for offline use.

**DAHITI API key**: DAHITI water level data requires a free account at https://dahiti.dgfi.tum.de/en/. Set `DAHITI_API_KEY` (v2) or `DAHITI_USERNAME` + `DAHITI_PASSWORD` (v1) before running. Without credentials CLAY skips DAHITI and continues with ERA5-Land, CRU TS, and NWM.

---

## Key data structures

**`MVSeries`** (`GLHE/CLAY/helpers.py`) — the central unit of data. A thin wrapper around `pd.Series` that carries:
- `dataset`: time-indexed `pd.Series`
- `unit`: `pint.Unit`
- `single_letter_code`: `"p"` (precip), `"e"` (evap), `"i"` (inflow), `"o"` (outflow), `"l"` (water level)
- `product_name`: e.g. `"ERA5-Land"`
- `xarray_dataarray`: original gridded `xr.DataArray` before spatial averaging

**`datasets_index`** in `CLAY_driver` — dict organizing all `MVSeries`:
- `"all"`: flat list of every loaded dataset
- `"grid"`: datasets that still need geodata attached
- `"slc"`: dict keyed by single-letter-code

---

## Architecture

```
HYLAK_ID
  └─> CLAY_driver.main()
        ├─> LakeExtraction      (lake_extraction.py)   — shapely polygon via hydrolakes.py
        ├─> data_check          (data_access/)          — download missing LocalData
        └─> For each data product (ERA5_Land, CRUTS, NWM):
              ├─> product_driver(polygon)               — fetch + spatially average
              └─> attach_geodata()                      — validate alignment
        ├─> combined_data_functions.py                  — merge, plot, export
        └─> outputs: CSV, PNG, shapefiles, config.json

LIME (panel serve GLHE/LIME/app.py, or GLHE.LIME.app.serve(output_dir))
  └─> app.py           — the whole page: load_lake(), series/caveats/table views, class Lime
  └─> globe.py + .js   — MapLibre globe (the only JS); every lake from PMTiles
  └─> gridded.py       — CLAY's gridded GeoTIFFs as coloured cells on the globe
  └─> jobs.ClayJob     — runs `python -m GLHE.LIME.run_clay <id>` in a subprocess
  └─> catalog / public — processed-lake list; public-mode guards
```

**Data products** are registered in `GLHE/CLAY/config/data_products.json` (the driver imports `GLHE.CLAY.data_access.<module_name>.<class_name>` from it) and their source URLs in `GLHE/CLAY/data_access/data_access_config/input_data.json`. A product that cannot be set up (missing module, credentials, bad spec) or whose data is missing is logged and skipped; it never stops the run. Adding a new data source means either:
- **A gridded precipitation/evaporation grid** (most new products): no code. Put the Zarr/NetCDF under `LocalData/` (on the server `~/glhe-data/LocalData`) and add a `data_products.json` entry with `"module_name": "gridded_product", "class_name": "GriddedProduct"` and a `"spec"` (`product_name`, `local_path`, `variables` `{name in file: "p"|"e"}`, optional `units`, optional S3 `cloud`). See `data_access/gridded_product.py`.
- **Anything else** (APIs, per-lake tables, points): a class in `GLHE/CLAY/data_access/` that subclasses `DataAccess`, implementing `verify_inputs()`, `attach_geodata()`, `product_driver(polygon, debug, run_cleanly)`, registered in both JSON config files.

**LIME** (`GLHE/LIME/`) is deliberately barebones: plain Panel widgets, default styling, no custom CSS or themes. Keep it that way. The only JavaScript is `globe.js`.
- `app.py` holds the whole page: `load_lake(folder)` reads a CLAY output folder into a dict, `series_view`/`caveats_view`/`table_view` turn it into tabs, and `class Lime` owns the widgets and callbacks. `build_app()` builds a page; `app.serve(output_dir)` serves it from Python.
- CLAY runs in a subprocess, one job per server (kept in `pn.state.cache`); each page polls it every second. `run_clay` prints `GLHE_OUTPUT_DIR=<path>` on success and the app loads that folder.
- HydroLAKES is a GeoParquet file read by `GLHE/CLAY/hydrolakes.py` (shared by CLAY and LIME): a local copy in `LocalData/` if present, else the GLHE data server, else `s3://glhe/HydroLAKES/HydroLAKES_polys_v10.parquet`, fetching only the rows needed. Override with env `GLHE_HYDROLAKES`. Rebuild it from the shapefile with `data_access/aws_zarr_templates/hydrolakes_to_parquet.py`. No GDAL Python bindings are needed.
- Lake picker: the sidebar search box uses `hydrolakes.search()` (an in-memory index of every lake's ID, name, country and area, preloaded by `app.warm_hydrolakes()`). The globe draws every lake from vector tiles (`HydroLAKES_polys_v10.pmtiles`, built by `aws_zarr_templates/hydrolakes_to_pmtiles.py`, URL from env `GLHE_LAKE_TILES_URL`, `""` = off); clicking a tile lake sets `LakeGlobe.picked` and selects it. Outlines are fetched in the background (`app.in_background`).
- Input data (HydroLAKES, `zarr/ERA5.zarr`, `zarr/CRUTS.zarr`) is read by `GLHE/CLAY/data_sources.py` from this machine (`LocalData/`), else the GLHE data server (`https://glhe.venumuddula.com/<path>`, served by the deploy), else S3 (`s3://glhe/<path>`); the relative path is the same on the server and in S3.
- Caveats belong to CLAY: queryearth rule files in `GLHE/CLAY/caveat_rules/` (`era5-land.geojson` vendored from vizqueryearth, `glhe.geojson` our own), matched by `GLHE/CLAY/caveats.py`. CLAY writes `{lake}_caveats.json` (`CAVEATS_DATA`); LIME recomputes them with the same module so rule edits apply to lakes already run. See `caveat_rules/README.md` to add rules.
- Public mode (`GLHE_PUBLIC=1`, `public.py`): run time limit, lake size cap, per-visitor cooldown, no reruns of processed lakes, cancel only by the starter, banner, no `/admin`. Deploy details in `deploy/README.md`.
- Dev data: `tests/LIME/fixtures/Fixture_Lake` is a synthetic CLAY output (regenerate with `make_fixture.py`).

**CALCITE pub/sub** — `OutputFileEvent` fires when CLAY writes a file; `DataProductRunEvent` fires per data product. The event bus is a singleton (`CALCITE/pubsub.py`). Subscribers register by event class name string.

---

## Testing

```bash
pytest                        # all tests
pytest -m "not slow"          # skip network/data tests
pytest tests/CLAY/            # CLAY tests only
pytest tests/test_CALCITE.py  # event bus tests
pytest tests/LIME/            # LIME app, views, jobs (no browser needed)
pytest tests/CLAY/test_caveats.py  # caveat rules
```

Test markers (`pytest.ini`): `slow`, `subsetting_data`. The slow tests hit real data downloads — skip them in normal dev cycles.

---

## Output files

CLAY writes all outputs to a per-lake directory under the configured output root (`GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]`):

| File | Description |
|------|-------------|
| `config.json` | Lake metadata + file paths (consumed by LIME) |
| `{lake}_monthly_avg_data.csv` | Merged time-series (all products) |
| `{lake}_all_data.png` | Multi-panel P/E/R plot |
| `Gridded_Data_{product}.zip` | GeoTIFF + metadata per product |
| `{lake}_NWM_lake_point.shp` | Lake center point shapefile |
| `{lake}_caveats.json` | Known data caveats matched for this lake (queryearth rules in `GLHE/CLAY/caveat_rules/`) |
| `Lake_Polygon.shp` | Lake boundary (EPSG:4326), written by CLAY; LIME uses it for the outline, area and region caveats |
| `GLHE_root.log` | Root logger |
| `{Product}_driver.log` | Per-product log |

---

## Configuration

| File | Purpose |
|------|---------|
| `GLHE/CLAY/globals.py` | Debug flags, directory paths, SLC mappings, pint unit registry |
| `GLHE/CLAY/config/data_products.json` | Which data products are enabled |
| `GLHE/CLAY/data_access_config/input_data.json` | Source URLs / access methods per product |
| `GLHE/CLAY/caveat_rules/*.geojson` | queryearth caveat rules (written per lake by CLAY, shown in LIME's Caveats tab) |
| env `GLHE_OUTPUT_DIR` | Lake folder LIME opens at start-up |
| env `GLHE_DATA_URL` | GLHE data server CLAY falls back to when a file is not in `LocalData/` (default `https://glhe.venumuddula.com`, `""` = skip; then S3). See `GLHE/CLAY/data_sources.py` |
| env `GLHE_LAKE_TILES_URL` | Lake vector tiles (PMTiles) drawn on the globe; default the S3 copy, `""` = none |

---

## Conventions

- All spatial data targets WGS 1984 / EPSG:4326.
- Units are tracked throughout using `pint` / `pint-xarray`. The custom unit registry is at `GLHE/CLAY/config/pint_unit_registry.txt`.
- Logging: every module uses `logging.getLogger(__name__)`. File handlers are added by each data product's `DataAccess` subclass constructor.
- Pickling: expensive intermediate results (e.g., xarray datasets) are pickled to the output directory for caching across reruns.
- Code is formatted with `black`.

---

## Data distribution strategy — an ongoing design tension

This is a core architectural challenge for GLHE. There are three distribution strategies in use, each with trade-offs:

| Strategy | Products that use it | Pro | Con |
|---|---|---|---|
| **S3 Zarr** (maintainer-hosted) | CRU TS, ERA5-Local | Zero user friction; lazy cloud streaming | Maintainer must convert + upload; can't use for licensed data |
| **API pull** | ERA5-Land, GloFAS, NWM, DAHITI | No hosting required | Requires user credentials; rate limits |
| **Local manual** | GLEAM | No redistribution concern | Poor UX; user must know to download |

### The S3 Zarr pipeline (maintainer workflow)

Template scripts live in `GLHE/CLAY/data_access/aws_zarr_templates/`:

1. **`netcdf_to_zarr.py`** — Convert raw NetCDF to chunked Zarr locally:
   - Rechunk to `{time: 10, lat: 100, lon: 100}` (good for spatial subsetting)
   - Compress with Blosc/zstd (`clevel=3`)
   - Save with `consolidated=True` for fast metadata reads
2. **`zarr_to_aws.py`** — Upload the local Zarr store to `s3://glhe/zarr/<product>.zarr`

These scripts are currently hardcoded one-offs. Before adding a new S3-hosted product, generalize them — or run them manually and update `input_data.json` with the new `cloud` path.

### The GLEAM problem

GLEAM's data policy requires each user to download their own copy from gleam.eu — it cannot be legally redistributed on a public S3 bucket. The current workaround (LocalData/ manual placement) is the only safe option unless the user has their own private S3. A future improvement: support a `user_cloud` override field in `input_data.json` so users who convert GLEAM to Zarr and upload to their own bucket can skip the LocalData step.

### Why this matters for new data products

When adding a new data source, decide upfront:
- **Can it be hosted on S3?** → Convert to Zarr, upload, add `"access_code": "local"` + `"cloud": "zarr/..."` to `input_data.json`
- **Does it have an API?** → Write a DataAccess class that calls the API, add `"access_code": "api"` entry
- **Licensed / no API?** → Use the GLEAM pattern: LocalData/ + clear docs on where to download

---

## Common tasks

**Add a new data source**: for a gridded p/e product, add a `GriddedProduct` spec entry to `data_products.json` (see above). Otherwise subclass `DataAccess` (see `GLHE/CLAY/data_access/data_access_parent_class.py`), then register in the two JSON config files. Use `ERA5_Land.py` or `GLEAM.py` as reference; CRU TS is a `GriddedProduct` spec.

**Change output directory**: Edit `GLHE/CLAY/globals.py` (`config["DIRECTORIES"]["OUTPUT_DIRECTORY"]`).

**Debug a specific data product**: Set `GLHE.CLAY.globals.config["DEBUG"] = True` and run `product_driver()` directly on the class instance.

**Run with cached data only**: Check that pickle files exist in the output directory before running; CLAY will detect and reuse them.

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
# on the server (rootless Podman + tailscale serve): see deploy/README.md
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
  └─> app.py LimeApp: sidebar (find lake, run CLAY, processed lakes), globe, tabs
  └─> state.AppState   — loaded lake (reads config.json + CSV + polygon)
  └─> jobs.ClayJob     — runs `python -m GLHE.LIME.run_clay <id>` in a subprocess
  └─> catalog / lake_lookup / caveats (queryearth rules in LIME/rules/)
  └─> components/      — globe (only JS: MapLibre), overview, series, water_balance, gridded, table, caveats
```

**Data products** are registered in `GLHE/CLAY/config/data_products.json` and their source URLs in `GLHE/CLAY/data_access_config/input_data.json`. Adding a new data source means:
1. Add a class in `GLHE/CLAY/data_access/` that subclasses `DataAccess`
2. Implement `verify_inputs()`, `attach_geodata()`, `product_driver(polygon, debug, run_cleanly)`
3. Register it in both JSON config files

**LIME** (`GLHE/LIME/`) is Python-first; the only JavaScript is `components/globe.js`.
- `app.py` builds the page (`build_app()`); `app.serve(output_dir)` serves it from Python.
- Every tab is a `make_*(state)` factory that watches `AppState` and redraws itself; components never talk to each other directly. Cross-component wiring (globe, caveats, header, CLAY job) lives in `LimeApp`.
- Colours/fonts/CSS come only from `theme.py`. Dark/light is chosen at page load (`?theme=light`); the header switch reloads the page.
- CLAY runs in a subprocess, one job per server (kept in `pn.state.cache`); the page polls it every 500 ms. `run_clay` prints `GLHE_OUTPUT_DIR=<path>` on success and the app loads that folder.
- HydroLAKES is a GeoParquet file read by `GLHE/CLAY/hydrolakes.py` (shared by CLAY and LIME's `lake_lookup`): a local copy in `LocalData/` if present, else `s3://glhe/HydroLAKES/HydroLAKES_polys_v10.parquet`, fetching only the rows needed. Override with env `GLHE_HYDROLAKES`. Rebuild it from the shapefile with `data_access/aws_zarr_templates/hydrolakes_to_parquet.py`. No GDAL Python bindings are needed.
- Caveats: queryearth rule files in `LIME/rules/` (`era5-land.geojson` vendored from vizqueryearth, `glhe.geojson` our own). See `rules/README.md` to add rules.
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
pytest tests/LIME/            # LIME app, views, caveats, jobs (no browser needed)
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
| `GLHE/LIME/theme.py` | LIME colours, fonts, CSS, chart defaults |
| `GLHE/LIME/rules/*.geojson` | queryearth caveat rules shown in the Caveats tab |
| env `GLHE_OUTPUT_DIR` | Lake folder LIME opens at start-up |
| env `GLHE_OUTPUT_ROOTS` | Extra folders (`:`-separated) LIME scans for processed lakes |

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

**Add a new data source**: Subclass `DataAccess` (see `GLHE/CLAY/data_access/data_access_parent_class.py`), then register in the two JSON config files. Use `ERA5_Land.py` or `CRUTS.py` as reference.

**Change output directory**: Edit `GLHE/CLAY/globals.py` (`config["DIRECTORIES"]["OUTPUT_DIRECTORY"]`).

**Debug a specific data product**: Set `GLHE.CLAY.globals.config["DEBUG"] = True` and run `product_driver()` directly on the class instance.

**Run with cached data only**: Check that pickle files exist in the output directory before running; CLAY will detect and reuse them.

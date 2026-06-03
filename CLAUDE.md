# CLAUDE.md — Global Lake Hydrology Explorer (GLHE)

## What this project is

GLHE is a Python package for fetching, processing, and visualizing lake-scale hydrology data (precipitation, evapotranspiration, runoff) for any lake in the world, identified by its HydroLAKES ID (HYLAK_ID). It is structured as three subpackages:

- **CLAY** — data access and processing (the core). Downloads and spatially averages global datasets over a lake polygon.
- **LIME** — interactive Plotly/Dash web dashboard for visualization (secondary, still rough).
- **CALCITE** — internal pub/sub event bus that decouples CLAY and LIME.

The project is prototype-stage research software. It has not been scientifically validated.

---

## Environment setup

Conda is the recommended approach (Python 3.12):

```bash
conda env create -f requirements/env-GLHE-basic.yml
conda activate glhe
pip install -e .
```

Platform-specific environment files live in `requirements/`. The basic one (`env-GLHE-basic.yml`) is most portable. `setup.py` is minimal and only registers the package name.

---

## Running the code

```python
import GLHE

# Lake ID lookup: https://glhe-fe.projects.earthengine.app/view/globallakehydrologyexplorer
HYLAK_ID = 67  # Great Salt Lake

clay = GLHE.CLAY.CLAY_driver.CLAY_driver()
output_dir = clay.main(HYLAK_ID)

# Optional: launch the web dashboard
# lime = GLHE.LIME.LIME_sample_dashboard.LIME_driver(output_dir)
```

See `sample_script.py` for the canonical example. The first run downloads data; subsequent runs use local cache (pickle files in the output directory).

**First-run data**: The code tries AWS S3 first, then falls back to Dropbox. To avoid a 2-hour download, manually download the `LocalData` folder from Dropbox and place it at `GLHE/CLAY/LocalData/` before the first run.

---

## Key data structures

**`MVSeries`** (`GLHE/CLAY/helpers.py`) — the central unit of data. A thin wrapper around `pd.Series` that carries:
- `dataset`: time-indexed `pd.Series`
- `unit`: `pint.Unit`
- `single_letter_code`: `"p"` (precip), `"e"` (evap), `"i"` (inflow), `"o"` (outflow)
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
        ├─> LakeExtraction      (lake_extraction.py)   — shapely polygon from HydroLAKES
        ├─> data_check          (data_access/)          — download missing LocalData
        └─> For each data product (ERA5_Land, CRUTS, NWM):
              ├─> product_driver(polygon)               — fetch + spatially average
              └─> attach_geodata()                      — validate alignment
        ├─> combined_data_functions.py                  — merge, plot, export
        └─> outputs: CSV, PNG, shapefiles, config.json

LIME_driver(output_dir)
  └─> Reads config.json written by CLAY
  └─> Dash app on localhost:8050
```

**Data products** are registered in `GLHE/CLAY/config/data_products.json` and their source URLs in `GLHE/CLAY/data_access_config/input_data.json`. Adding a new data source means:
1. Add a class in `GLHE/CLAY/data_access/` that subclasses `DataAccess`
2. Implement `verify_inputs()`, `attach_geodata()`, `product_driver(polygon, debug, run_cleanly)`
3. Register it in both JSON config files

**CALCITE pub/sub** — `OutputFileEvent` fires when CLAY writes a file; `DataProductRunEvent` fires per data product. The event bus is a singleton (`CALCITE/pubsub.py`). Subscribers register by event class name string.

---

## Testing

```bash
pytest                        # all tests
pytest -m "not slow"          # skip network/data tests
pytest tests/CLAY/            # CLAY tests only
pytest tests/test_CALCITE.py  # event bus tests
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
| `Lake_Polygon.shp` | Lake boundary shapefile |
| `GLHE_root.log` | Root logger |
| `{Product}_driver.log` | Per-product log |

---

## Configuration

| File | Purpose |
|------|---------|
| `GLHE/CLAY/globals.py` | Debug flags, directory paths, SLC mappings, pint unit registry |
| `GLHE/CLAY/config/data_products.json` | Which data products are enabled |
| `GLHE/CLAY/data_access_config/input_data.json` | Source URLs / access methods per product |
| `GLHE/LIME/config/config.json` | Path to CLAY output dir (written by CLAY) |
| `GLHE/LIME/config/local_data_files.json` | Mapbox token |

---

## Conventions

- All spatial data targets WGS 1984 / EPSG:4326.
- Units are tracked throughout using `pint` / `pint-xarray`. The custom unit registry is at `GLHE/CLAY/config/pint_unit_registry.txt`.
- Logging: every module uses `logging.getLogger(__name__)`. File handlers are added by each data product's `DataAccess` subclass constructor.
- Pickling: expensive intermediate results (e.g., xarray datasets) are pickled to the output directory for caching across reruns.
- Code is formatted with `black`.

---

## Common tasks

**Add a new data source**: Subclass `DataAccess` (see `GLHE/CLAY/data_access/data_access_parent_class.py`), then register in the two JSON config files. Use `ERA5_Land.py` or `CRUTS.py` as reference.

**Change output directory**: Edit `GLHE/CLAY/globals.py` (`config["DIRECTORIES"]["OUTPUT_DIRECTORY"]`).

**Debug a specific data product**: Set `GLHE.CLAY.globals.config["DEBUG"] = True` and run `product_driver()` directly on the class instance.

**Run with cached data only**: Check that pickle files exist in the output directory before running; CLAY will detect and reuse them.

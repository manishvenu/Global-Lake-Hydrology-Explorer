# Usage

## Installation

Clone the repository and set up the conda environment:

```console
$ git clone https://github.com/manishvenu/Global-Lake-Hydrology-Explorer.git
$ cd Global-Lake-Hydrology-Explorer
```

Create the conda environment (Linux recommended; Windows works, Mac untested):

```console
$ conda env create -f environment.yml
$ conda activate glhe
```

The `environment.yml` installs all dependencies and runs `pip install -e .[dev]`
automatically — no separate pip step needed.

## Data requirements (first run)

On the first run GLHE automatically downloads required datasets from AWS S3.
After a successful first run, cached pickle files are reused, so the download
only happens once.

To pre-cache all S3 data for offline use, call once before running:

```python
GLHE.CLAY.data_access.data_check.download_for_offline()
```

## Quick start

Find your lake's HYLAK ID using the
[GLHE map tool](https://glhe-fe.projects.earthengine.app/view/globallakehydrologyexplorer).

```python
import GLHE

HYLAK_ID = 67  # Great Salt Lake

# Step 1: run the data access layer (CLAY)
clay = GLHE.CLAY.CLAY_driver.CLAY_driver()
output_dir = clay.main(HYLAK_ID)

# Step 2 (optional): launch the web dashboard (LIME)
# lime = GLHE.LIME.LIME_sample_dashboard.LIME_driver(output_dir)
```

See `sample_script.py` in the repository root for the full runnable example.

The LIME dashboard (when enabled) is served at `http://localhost:8050`.

## API keys and credentials

### DAHITI (water level)

DAHITI provides satellite-altimetry-derived water surface elevation for lakes
worldwide.  It requires a free account — register at
<https://dahiti.dgfi.tum.de/en/>.

**API v2 (preferred)** — set one environment variable:

```console
$ export DAHITI_API_KEY=your_api_key_here
```

**API v1 (fallback)** — set username and password instead:

```console
$ export DAHITI_USERNAME=your_username
$ export DAHITI_PASSWORD=your_password
```

If neither is set CLAY logs a warning and continues without water level data
— the other products (ERA5-Land, CRU TS, NWM, GloFAS, GLEAM) are unaffected.

### GloFAS (river discharge)

GloFAS uses the same CDS API credentials as ERA5-Land.  No additional
environment variables are needed — ensure `~/.cdsapirc` is configured.

Reading the downloaded GRIB2 files requires `cfgrib`, which is included in the
conda environment:

```console
$ conda install -c conda-forge cfgrib   # if updating an existing environment
```

### GLEAM (actual lake evaporation)

GLEAM does not have a public API.  Download the monthly NetCDF files manually:

1. Register at <https://www.gleam.eu> (free).
2. Download `Ew_*_GLEAM_v3.*.nc` (open-water evaporation) or
   `Ea_*_GLEAM_v3.*.nc` (total actual ET) for the years you need.
3. Place the files in `GLHE/CLAY/LocalData/GLEAM/`.

CLAY will raise a warning and skip GLEAM if no files are found, so the other
products are unaffected.

---

## Configuration

### Output directory

The output root is set in `GLHE/CLAY/globals.py`:

```python
config["DIRECTORIES"]["OUTPUT_DIRECTORY"]
```

Each run creates a sub-directory named after the lake.

### Data products

Which data sources are active is controlled by
`GLHE/CLAY/config/data_products.json`. Source URLs and access methods are
in `GLHE/CLAY/data_access_config/input_data.json`.

## Output files

After a successful run the output directory contains:

| File | Description |
|------|-------------|
| `config.json` | Lake metadata and file paths (read by LIME) |
| `{lake}_Data.csv` | Merged monthly time-series for all data products |
| `{lake}_products_plot.png` | Multi-panel P / E / inflow / water level plot |
| `{lake}_water_balance.csv` | P, E, P−E (mm/month), inflow (m³/month), water level (m) |
| `{lake}_trend_stats.csv` | Mann-Kendall trend test and Sen's slope per variable |
| `{lake}_gridded_data_layers_on_{date}.zip` | GeoTIFF snapshot for each gridded product |
| `{lake}_NWM_lake_point.shp` | Lake centre point shapefile |
| `Lake_Polygon.shp` | Lake boundary shapefile |
| `GLHE_root.log` | Root logger output |
| `{Product}_driver.log` | Per-data-product log |

## Running the tests

```console
$ pytest                      # all tests
$ pytest -m "not slow"        # skip network / data download tests
$ pytest tests/CLAY/          # CLAY module tests only
```

Architecture
============

GLHE is split into three subpackages that mirror the AWIPS design philosophy:
CLAY handles data, LIME handles display, and CALCITE handles communication
between them.

.. code-block:: text

    HYLAK_ID (lake identifier)
      └─> CLAY_driver.main()
            ├─> LakeExtraction      — lake polygon from HydroLAKES shapefile
            ├─> data_check          — download missing LocalData on first run
            └─> For each data product (ERA5_Land, CRUTS, NWM):
                  ├─> product_driver(polygon)   — fetch + spatially average
                  └─> attach_geodata()          — validate grid alignment
            ├─> combined_data_functions         — merge, plot, export
            └─> outputs: CSV, PNG, shapefiles, config.json

    LIME_driver(output_dir)
      └─> Reads config.json produced by CLAY
      └─> Dash web app on localhost:8050

CLAY — data access layer
------------------------

The core of the project. Named after the sedimentary mineral, it is
described in the README as "the hard (useful) part."

``CLAY_driver``
    Orchestrates the full data pipeline. Keeps a ``datasets_index`` dict
    with three views of the loaded datasets: ``"all"`` (flat list),
    ``"grid"`` (datasets awaiting geodata attachment), and ``"slc"``
    (keyed by single-letter code).

``MVSeries``
    The central data structure — a thin wrapper around ``pd.Series`` that
    carries the time series together with its unit (``pint.Unit``), data
    product name, variable name, and the original ``xr.DataArray`` grid.

    .. list-table::
       :header-rows: 1

       * - Attribute
         - Type
         - Meaning
       * - ``dataset``
         - ``pd.Series``
         - Monthly time-indexed values
       * - ``unit``
         - ``pint.Unit``
         - Physical unit (e.g. mm/month)
       * - ``single_letter_code``
         - ``str``
         - ``"p"`` precip, ``"e"`` evap, ``"i"`` inflow, ``"o"`` outflow
       * - ``product_name``
         - ``str``
         - e.g. ``"ERA5-Land"``
       * - ``xarray_dataarray``
         - ``xr.DataArray``
         - Original gridded data before spatial averaging

``DataAccess`` (abstract base class)
    Every data source subclasses ``DataAccess`` and must implement:

    - ``verify_inputs()`` — validate parameters
    - ``product_driver(polygon, debug, run_cleanly)`` — fetch and return ``MVSeries``
    - ``attach_geodata()`` — return ``"grid"`` or ``"complete"``

    Current implementations: ``ERA5_Land``, ``CRUTS``, ``NWM``, ``DAHITI``.

``LakeExtraction``
    Reads the HydroLAKES shapefile and returns the ``shapely`` polygon for
    the requested lake, used for spatial subsetting of all gridded products.

``xarray_helpers``
    Utility functions for spatially averaging a global ``xr.DataArray`` over
    a lake polygon, unit conversion, and conversion to ``MVSeries``.

``combined_data_functions``
    Merges all ``MVSeries`` into a single ``pd.DataFrame``, generates the
    multi-panel PNG, and writes all output files.

LIME — visualization layer
--------------------------

A Plotly/Dash web application. It reads the ``config.json`` written by
CLAY and renders:

- Lake location map (Folium)
- Time-series table of monthly P/E/R
- Gridded data validation maps (Holoviews)
- Lake point validation overlay

Currently described as "a mess" and is lower priority than CLAY.

CALCITE — message bus
---------------------

A singleton event bus (``pubsub.EventBus``) that allows CLAY data products
to notify the driver about file outputs and run status without tight
coupling. Topics are matched by event class name.

Key events defined in ``events.py``:

- ``OutputFileEvent`` — fires when CLAY writes a file
- ``DataProductRunEvent`` — fires when a data product run completes
- ``TestEvent`` — for unit tests

Adding a new data source
------------------------

1. Create a new class in ``GLHE/CLAY/data_access/`` that subclasses
   ``DataAccess`` (use ``ERA5_Land.py`` as a reference).
2. Implement the three required methods.
3. Register the product in ``GLHE/CLAY/config/data_products.json``.
4. Add its source URL / access method to
   ``GLHE/CLAY/data_access_config/input_data.json``.

Data flow and units
-------------------

All spatial data targets WGS 1984 / EPSG:4326. Units are tracked end-to-end
using ``pint`` and ``pint-xarray``. The custom unit registry lives in
``GLHE/CLAY/config/pint_unit_registry.txt``.

Expensive intermediate results (xarray datasets, lake polygons) are pickled
into the output directory so repeated runs skip redundant downloads.

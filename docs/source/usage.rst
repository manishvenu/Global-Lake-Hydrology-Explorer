Usage
=====

Installation
------------

Clone the repository and set up the conda environment:

.. code-block:: console

    $ git clone https://github.com/manishvenu/Global-Lake-Hydrology-Explorer.git
    $ cd Global-Lake-Hydrology-Explorer

Create the conda environment (Linux recommended; Windows works, Mac untested):

.. code-block:: console

    $ conda env create -f requirements/env-GLHE-basic.yml
    $ conda activate glhe

Install the package in editable mode:

.. code-block:: console

    $ pip install -e .

Data requirements (first run)
------------------------------

On the first run GLHE automatically downloads required datasets. The code
tries AWS S3 first, then falls back to Dropbox. The Dropbox path can take up
to 2 hours depending on your connection.

To skip the long download, manually download the ``LocalData`` folder from
the `GLHE Dropbox repository <https://www.dropbox.com/scl/fo/d9a8t8rs05qjr9dnzdzvw/AOIR2cebgd4tb3u5rjOm4RQ?rlkey=z8pd6py7ec1wwwt0rahae77zb&st=c10pjgqf&dl=0>`_
and extract it as ``GLHE/CLAY/LocalData/``. After a successful first run the
cached pickle files are reused, so the download only happens once.

Quick start
-----------

Find your lake's HYLAK ID using the
`GLHE map tool <https://glhe-fe.projects.earthengine.app/view/globallakehydrologyexplorer>`_.

.. code-block:: python

    import GLHE

    HYLAK_ID = 67  # Great Salt Lake

    # Step 1: run the data access layer (CLAY)
    clay = GLHE.CLAY.CLAY_driver.CLAY_driver()
    output_dir = clay.main(HYLAK_ID)

    # Step 2 (optional): launch the web dashboard (LIME)
    # lime = GLHE.LIME.LIME_sample_dashboard.LIME_driver(output_dir)

See ``sample_script.py`` in the repository root for the full runnable example.

The LIME dashboard (when enabled) is served at ``http://localhost:8050``.

Configuration
-------------

Output directory
~~~~~~~~~~~~~~~~

The output root is set in ``GLHE/CLAY/globals.py``:

.. code-block:: python

    config["DIRECTORIES"]["OUTPUT_DIRECTORY"]

Each run creates a sub-directory named after the lake.

Data products
~~~~~~~~~~~~~

Which data sources are active is controlled by
``GLHE/CLAY/config/data_products.json``. Source URLs and access methods are
in ``GLHE/CLAY/data_access_config/input_data.json``.

Output files
------------

After a successful run the output directory contains:

.. list-table::
   :header-rows: 1

   * - File
     - Description
   * - ``config.json``
     - Lake metadata and file paths (read by LIME)
   * - ``{lake}_monthly_avg_data.csv``
     - Merged monthly time-series for all data products
   * - ``{lake}_all_data.png``
     - Multi-panel precipitation / evapotranspiration / runoff plot
   * - ``Gridded_Data_{product}.zip``
     - GeoTIFF + metadata for each data product
   * - ``{lake}_NWM_lake_point.shp``
     - Lake centre point shapefile
   * - ``Lake_Polygon.shp``
     - Lake boundary shapefile
   * - ``GLHE_root.log``
     - Root logger output
   * - ``{Product}_driver.log``
     - Per-data-product log

Running the tests
-----------------

.. code-block:: console

    $ pytest                      # all tests
    $ pytest -m "not slow"        # skip network / data download tests
    $ pytest tests/CLAY/          # CLAY module tests only

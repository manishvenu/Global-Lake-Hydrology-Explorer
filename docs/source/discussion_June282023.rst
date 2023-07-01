.. _June282023:
Discussion: June 28, 2023
=========================

This is the first discussion. How this'll work: They'll be a status section and then just long running issues or debates.

Status
------------

Since this is the first one, it'll be longer.

1. Data access for ERA5 and CRUTS as a prototype of P, E. The big hard stuff are the helper functions.
Massaging this data into a standard format is a lot of work and requires small changes in a lot of places.Helpers.py is the real haul here.

2. Getting a specific lake by ID into a usable polygon (Shapely Library). Uses pretty high power libraries like ogc

3. Product Driver functions were created. These run the data access and helper functions until the data is standardized.

4. Subsetting xarray to netcdf was also a big deal. That was done in lake_extraction.py simply, as a prototype, copied from some person (see CREDITS)

5. Plotting and presenting the data was also prototyped. Creates (or should create) a plot, csv, and rasters of the data.

6. Little sick of writing the whole status of the project and say check the history at this point?

Planned Products
-----------------

Used ChatGPT and myself to come up with planned products, should be called Products.md. It's a quick starter list of cool things we want.


Dropbox
------------

I'm going to put the data in dropbox. It's an app folder, but that's pretty unnecessary.
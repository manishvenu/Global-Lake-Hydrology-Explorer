# Discussion: June 28, 2023

First discussion entry. Format: a status section followed by running issues or debates.

## Status

Since this is the first one, it'll be longer.

1. Data access for ERA5 and CRUTS as a prototype of P, E. The big hard stuff are the helper functions.
   Massaging this data into a standard format is a lot of work and requires small changes in a lot of places.
   `helpers.py` is the real haul here.

2. Getting a specific lake by ID into a usable polygon (Shapely). Uses high-power libraries like OGC.

3. Product Driver functions were created. These run the data access and helper functions until the data is
   standardized.

4. Subsetting xarray to netcdf was a big deal. Done in `lake_extraction.py` as a prototype (see CREDITS).

5. Plotting and presenting the data was also prototyped. Creates a plot, CSV, and rasters of the data.

## Planned Products

Used ChatGPT + myself to come up with planned products, now in `products.md`.

## Dropbox

Data will be stored in Dropbox as a backup. Won't be accessed unless AWS S3 is unavailable.

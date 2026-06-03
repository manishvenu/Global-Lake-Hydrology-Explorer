# Data Sources

## Lakes

Lake boundaries come from the [HydroLAKES database](https://www.hydrosheds.org/products/hydrolakes/),
a global shapefile of lakes with surface area above 10 ha.

## ERA5-Land

Precipitation and evaporation data from the
[ERA5-Land reanalysis](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land-monthly-means)
produced by ECMWF. Good global coverage but has known regional biases;
future versions will add quality flags using WorldClim or a similar reference dataset.

## CRU TS

The [CRU TS dataset](https://crudata.uea.ac.uk/cru/data/hrg/) from the
Climatic Research Unit (University of East Anglia) and the Met Office.
Included primarily because of its long temporal record. It is a relatively
coarse dataset and will likely be replaced by TerraClimate in a future release.

## NWM (National Water Model)

[NWM retrospective streamflow](http://www.hydroshare.org/resource/830b603665cc4ac0893d5449badd422d)
via the NOAA National Water Model. US-only coverage. Accessed via Zarr files
on AWS. Based on work by Castronova, A. M. (2023).

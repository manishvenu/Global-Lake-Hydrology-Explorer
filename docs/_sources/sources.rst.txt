Sources
=======

Right now, sources of the code are listed here. Hopefully we will have more robust explanations and reasoning behind them in the future. Don't trust anything.

Lakes
------------

To find the lakes we wanted, we use the HydroLakes database.

The database is a shapefile that takes lake above 10 ha.

ERA
------------

We use the ERA5 reanalysis from the ECMWF. We take precipitation and evaporation data from there.
It's not robust in certain areas, but it's the best we have for now. We'll add flags & selective process based on WorldClim or a dataset like that in the future.

CRUTS
------------

We use the CRUTS dataset primarily because it goes back so far. It's frankly a not great dataset, and will be removed when TerraClimate gets moved in.

NWM
----
We use this because it is easily accessible for the US. It's not great b/c not global, but it's the best we have for now.
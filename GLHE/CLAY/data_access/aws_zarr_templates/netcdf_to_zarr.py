import xarray as xr
import zarr

# Open NetCDF with spatial chunking
print("Opening NetCDF...")
ds = xr.open_mfdataset(
    "/mnt/c/Users/manis/Documents/GLHE/GLHE/CLAY/LocalData/cruts_pet_pre_4.07_1901_2022.nc",
    chunks="auto",
)
ds = ds.chunk({"time": 10, "lat": 100, "lon": 100})  # Ensure uniform chunking

# Apply Blosc compression
print("Applying Blosc compression...")
encoding = {
    var: {"compressor": zarr.Blosc(cname="zstd", clevel=3)} for var in ds.data_vars
}

# Save to Zarr
print("Saving to Zarr...")
ds.to_zarr(
    "/mnt/c/Users/manis/Documents/GLHE/GLHE/CLAY/LocalData/zarr/CRUTS_v2/output.zarr",
    encoding=encoding,
    consolidated=True,
)

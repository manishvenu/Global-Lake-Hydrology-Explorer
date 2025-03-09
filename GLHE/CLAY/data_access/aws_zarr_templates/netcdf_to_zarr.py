import xarray as xr
import zarr

# Open NetCDF with spatial chunking
print("Opening NetCDF...")
ds = xr.open_mfdataset(
    "/mnt/c/Users/manis/Documents/GLHE/GLHE/CLAY/LocalData/ERA5/data*",
    chunks="auto",
)
ds = ds.chunk(
    {"valid_time": 10, "latitude": 100, "longitude": 100}
)  # Ensure uniform chunking

# Apply Blosc compression
print("Applying Blosc compression...")
encoding = {
    var: {"compressor": zarr.Blosc(cname="zstd", clevel=3)} for var in ds.data_vars
}

# Save to Zarr
print("Saving to Zarr...")
ds.to_zarr(
    "/mnt/c/Users/manis/Documents/GLHE/GLHE/CLAY/LocalData/zarr/ERA5/output.zarr",
    encoding=encoding,
    consolidated=True,
)

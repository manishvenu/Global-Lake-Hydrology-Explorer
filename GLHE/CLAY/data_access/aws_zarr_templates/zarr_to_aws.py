import s3fs
import xarray as xr

fs = s3fs.S3FileSystem()

# Open the Zarr dataset locally
print("Opening Zarr...")
ds = xr.open_zarr(
    "/mnt/c/Users/manis/Documents/GLHE/GLHE/CLAY/LocalData/zarr/CRUTS_v2/output.zarr"
)
# Define S3 path
print("Defining S3 path...")
zarr_s3_path = "s3://glhe/zarr/CRUTS.zarr"

# Upload Zarr dataset
print("Uploading Zarr...")
ds.to_zarr(zarr_s3_path, storage_options={"s3": {"anon": False}}, consolidated=True)

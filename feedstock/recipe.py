import xarray as xr
import fsspec
import earthaccess
import os
from datetime import datetime
import matplotlib.pyplot as plt
client = Client()
client
# Shuttle Radar Topography Mission (SRTM) Global 1-Arc Second Digital Elevation Model (DEM) is saved to zarr
# Log in to NASA Earthdata
auth = earthaccess.login(strategy="interactive")

# Get an authenticated session for requests
session = earthaccess.get_requests_https_session()

# Define root storage and product name
base_dir = "https://nyu1.osn.mghpcc.org"
root_dir = "leap-pangeo-pipeline"
product_name = "SRTMGL1_DEM"

# Search for global SRTM NetCDF files
search_results = earthaccess.search_data(
    short_name="SRTMGL1_NC",
    bounding_box=(-180, -90, 180, 90)  # Global extent
)

def open_remote_file(file_url: str) -> xr.Dataset:
    """
    Open a remote NetCDF file from NASA's cloud using authentication.
    """
    try:
        # Open dataset remotely using fsspec with authentication
        ds = xr.open_dataset(
            fsspec.open(file_url, headers=session.headers).open(), 
            engine="h5netcdf"
        )

        # Ensure required variables exist
        required_vars = ["lat", "lon", "SRTMGL1_DEM"]
        missing_vars = [var for var in required_vars if var not in ds.variables]

        if missing_vars:
            raise KeyError(f"Missing variables in dataset: {missing_vars}")

        # Assign a fixed time dimension
        time = datetime(2000, 2, 11)

        # Create new xarray dataset with time coordinate
        ds = xr.Dataset(
            {"elevation": (["lat", "lon"], ds["SRTMGL1_DEM"].values)},
            coords={"time": [time], "lat": ds["lat"].values, "lon": ds["lon"].values}
        )

        return ds

    except Exception as e:
        print(f"Error opening {file_url}: {e}")
        return None

# Initialize list for datasets
datasets = []

# Extract dataset URLs and read remotely
for result in search_results:  
    file_url = None
    for url_entry in result["umm"]["RelatedUrls"]:
        if "URL" in url_entry and url_entry["URL"].endswith(".nc"):
            file_url = url_entry["URL"]
            break
    
    if file_url:
        ds = open_remote_file(file_url)
        if ds:
            datasets.append(ds)

# Concatenate datasets along the time dimension
if len(datasets) > 0:
    cds = xr.concat(datasets, dim="time")
else:
    raise ValueError("No valid datasets found for concatenation.")

# Save to Zarr format for fast access
fs = s3fs.S3FileSystem(
    key="", secret="", client_kwargs={"endpoint_url": root_dir}
)
mapper = fs.get_mapper(os.path.join(root_dir,product_name,f"{product_name}.zarr"))

cds.chunk({"time": 1, "lat": 3601, "lon": 3601}).to_zarr(
    mapper, mode="w", consolidated=True
)

store = os.path.join(base_dir,root_dir,f"{product_name}.zarr")
cds.to_zarr(store, mode="w", consolidated=True)
ds = xr.open_dataset(store, engine="zarr", chunks={})
ds.isel(time=0).lwe_thickness.plot()

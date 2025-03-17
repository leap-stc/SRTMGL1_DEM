# switched to dask 'recipe'
# input files were transfered in `transfer.sh` to LEAP OSN due to very slow server

import requests
import rioxarray as rxr
import xarray as xr
import numpy as np
import re
from rasterio.io import MemoryFile
from obstore.fsspec import AsyncFsspecStore
from obstore.store import S3Store
import pandas as pd
from dask.distributed import Client
import s3fs
import os
import requests
import matplotlib.pyplot as plt
import earthaccess
client = Client()
client

"""
TELLUS → NASA's GRACE Tellus project; GRAC → GRACE satellite mission;
L3 → Level-3 processed data; JPL → processed by NASA JPL; 
RL06 → Release 06 (latest); LND → land data; v04 → Version 4.
"""
auth = earthaccess.login(strategy="interactive") 
base_dir = "https://nyu1.osn.mghpcc.org"
root_dir = "leap-pangeo-pipeline"
product_name="GRACE-GRACE-FO-TWS"
search_results = earthaccess.search_data(
    short_name="TELLUS_GRAC_L3_JPL_RL06_LND_v04", 
    provider="POCLOUD",
    bounding_box=(-180, -90, 180, 90),
)
def read_file(grace_file_url: str,root_dir :str)-> xr.Dataset:
    file_name = os.path.basename(grace_file_url)
    file_path = os.path.join(root_dir, file_name)
    # Download the dataset if it doesn't exist
    if not os.path.exists(file_path):
        response = requests.get(grace_file_url, stream=True)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            print(f"Download complete: {file_path}")
        else:
            raise ConnectionError(f"Failed to download the file. HTTP Status: {response.status_code}")

    # Open the GRACE dataset
    ds = xr.open_dataset(file_path)
    
    # Ensure the dataset has the expected variables
    required_vars = ["lat", "lon", "time", "lwe_thickness", "uncertainty"]
    missing_vars = [var for var in required_vars if var not in ds.variables]
    
    if missing_vars:
        raise KeyError(f"Missing variables in dataset: {missing_vars}")

    # Extract relevant data
    lat = ds["lat"].values
    lon = ds["lon"].values
    time = ds["time"].values
    tws = ds["lwe_thickness"].values  # Total Water Storage Anomaly in cm
    uncertainty = ds["uncertainty"].values  # Uncertainty in cm

    # Ensure that tws and uncertainty are properly shaped
    if tws.shape[0] != len(time) or tws.shape[1] != len(lat) or tws.shape[2] != len(lon):
        raise ValueError(f"Unexpected TWS dimensions: {tws.shape}, expected ({len(time)}, {len(lat)}, {len(lon)})")

    # remove time dimension TWS & Uncertainty data (one dimension in shape is removed (1,x,y))
    latest_tws = tws[-1, :, :]
    latest_uncertainty = uncertainty[-1, :, :]

    # Create a new xarray Dataset with structured dimensions
    ds = xr.Dataset(
        {
            "lwe_thickness": (["lat", "lon"], latest_tws),
            "uncertainty": (["lat", "lon"], latest_uncertainty)
        },
        coords={
            "time": time[-1],  # Latest time step
            "lat": lat,
            "lon": lon
        }
    )
    # Ensure the file is deleted after processing
    if os.path.exists(file_path):
            os.remove(file_path)
    return ds
datasets = []  # Initialize an empty list to store valid xarray Datasets

# Select the first search result
for result in search_results:

    # Extract dataset URL from 'RelatedUrls'
    grace_file_url = None
    for url_entry in result["umm"]["RelatedUrls"]:
        if "URL" in url_entry and url_entry["URL"].endswith(".nc"):
            grace_file_url = url_entry["URL"]
            break
    print(grace_file_url)
    ds=read_file(grace_file_url,root_dir)
    datasets.append(ds)
    

# **Ensure At Least One Valid Dataset Exists**
datasets = [ds for ds in datasets if isinstance(ds, xr.Dataset)]  
if len(datasets) > 0:
    cds = xr.concat(datasets, dim="time")
else:
    raise ValueError("No valid datasets found for concatenation.")


# check RT
fs = s3fs.S3FileSystem(
    key="", secret="", client_kwargs={"endpoint_url": root_dir}
)
mapper = fs.get_mapper(os.path.join(root_dir,product_name,f"{product_name}.zarr"))

cds.chunk({"time": 100, "lat": 360, "lon": 720}).to_zarr(
    mapper, mode="w", consolidated=True
)

store = os.path.join(base_dir,root_dir,f"{product_name}.zarr")
cds.to_zarr(store, mode="w", consolidated=True)
ds = xr.open_dataset(store, engine="zarr", chunks={})
ds.isel(time=0).lwe_thickness.plot()


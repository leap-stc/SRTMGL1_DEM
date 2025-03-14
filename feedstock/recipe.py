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

client = Client()
client

# Retrieve VRT file to extract available `.tif` tiles
base_url = "https://libdrive.ethz.ch/index.php/s/cO8or7iOe5dT2Rt/download?path=/"
vrt_file_url = base_url + "ETH_GlobalCanopyHeight_10m_2020_mosaic_Map.vrt"
response = requests.get(vrt_file_url)
file_names = re.findall(r'3deg_cogs/ETH_GlobalCanopyHeight_10m_2020_[NS]\d{2}[EW]\d{3}_Map\.tif', response.text)

read_store = S3Store(
    "leap-pangeo-manual/CanopyHeights-GLAD",
    aws_endpoint="https://nyu1.osn.mghpcc.org",
    access_key_id="",
    secret_access_key="",
)

def read_file(file_name: str,base_url: str)-> xr.Dataset:
    """
    Reads a canopy height dataset file and its corresponding standard deviation file 
    directly from a remote URL without downloading them.

    Parameters:
        file_name (str): The name of the canopy height raster file (e.g., "ETH_GlobalCanopyHeight_10m_2020_N00E096_Map.tif").
        base_url (str): The base URL where the dataset is hosted.

    Returns:
        xr.Dataset: A dataset containing `canopy_height`, `std`, `lat`, `lon`, and `time` variables.
    """
    std_file_name = file_name.replace("_Map.tif", "_Map_SD.tif")
    mean_url = base_url + file_name
    std_url = base_url + std_file_name
    response_mean = requests.get(mean_url, stream=True)
    response_std = requests.get(std_url, stream=True)
    if response_mean.status_code == 200 and response_std.status_code == 200:
        with MemoryFile(response_mean.content) as memfile_mean, MemoryFile(response_std.content) as memfile_std:
            with memfile_mean.open() as src_mean, memfile_std.open() as src_std:
                
                # Read into xarray using rioxarray
                da_mean = rxr.open_rasterio(src_mean).squeeze()
                da_std = rxr.open_rasterio(src_std).squeeze()
                ch = da_mean.values
                std = da_std.values
                ch = np.where(ch == 255, np.nan, ch)
                std = np.where(std == 255, np.nan, std)
                lon_ch, lat_ch = np.meshgrid(da_mean.x.values, da_mean.y.values)
                date = pd.to_datetime(f"2020-1", format="%Y-%j")#only one date as it is static variable
                ds = xr.Dataset(
                    {
                        "canopy_height": (["y", "x"], ch),
                        "std": (["y", "x"], std)
                    },
                    coords={
                        "time": date,
                        "lat": (["y", "x"], lat_ch),
                        "lon": (["y", "x"], lon_ch)
                    }
                )

                return ds
                




# Read all files and store datasets
datasets = []
for file_name in file_names:
    ds = read_file(file_name, base_url)
    datasets.append(ds)

# Combine datasets along time dimension
if datasets:
    cds = xr.concat(datasets, dim="time")
    
fs = s3fs.S3FileSystem(
    key="", secret="", client_kwargs={"endpoint_url": "https://nyu1.osn.mghpcc.org"}
)

mapper = fs.get_mapper("leap-pangeo-pipeline/
CanopyHeights-GLAD/
CanopyHeights-GLAD.zarr")

cds.chunk({"time": 100, "lat": 360, "lon": 720}).to_zarr(
    mapper, mode="w", consolidated=True
)

# check RT


store = "https://nyu1.osn.mghpcc.org/leap-pangeo-pipeline/
CanopyHeights-GLAD/
CanopyHeights-GLAD.zarr"
ds = xr.open_dataset(store, engine="zarr", chunks={})
ds.isel(time=0).canopy_height.plot()

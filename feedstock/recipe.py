import xarray as xr
import fsspec
import earthaccess
import os
from datetime import datetime
import matplotlib.pyplot as plt
import warnings
import s3fs
from dask.distributed import Client

# ───────────────────────────────────────────────
# 1. Setup
# ───────────────────────────────────────────────
warnings.filterwarnings("ignore")
client = Client()
print(client)

# ───────────────────────────────────────────────
# 2. Login to NASA Earthdata
# ───────────────────────────────────────────────
auth = earthaccess.login(strategy="interactive")
session = earthaccess.get_requests_https_session()

# ───────────────────────────────────────────────
# 3. Define Output Zarr Path (OSN)
# ───────────────────────────────────────────────
base_dir = "https://nyu1.osn.mghpcc.org"
root_dir = "leap-pangeo-pipeline"
product_name = "SRTMGL1_DEM"
zarr_path = os.path.join(base_dir,root_dir, f"{product_name}.zarr")
mapper_path = os.path.join(root_dir, f"{product_name}.zarr")
# Use s3fs for writing
fs = s3fs.S3FileSystem(
    key="", secret="", client_kwargs={"endpoint_url": base_dir}
)
store = fs.get_mapper(mapper_path)


# ───────────────────────────────────────────────
# 4. Search for Global SRTMGL1 NetCDF Files
# ───────────────────────────────────────────────
search_results = earthaccess.search_data(
    short_name="SRTMGL1_NC",
    bounding_box=(-180, -90, 180, 90)
)

# ───────────────────────────────────────────────
# 5. Define File Reader
# ───────────────────────────────────────────────
def open_remote_file(file_url: str) -> xr.Dataset:
    try:
        ds = xr.open_dataset(
            fsspec.open(file_url, headers=session.headers).open(),
            engine="h5netcdf"
        )
        if "SRTMGL1_DEM" not in ds.variables:
            raise KeyError("Missing elevation variable 'SRTMGL1_DEM'")

        time = datetime(2000, 2, 11)  # Static acquisition time
        return xr.Dataset(
            {"elevation": (["lat", "lon"], ds["SRTMGL1_DEM"].values)},
            coords={
                "tile_id": [i],#must be unique
                "time": [datetime(2000, 2, 11)],
                "lat": ds["lat"].values,
                "lon": ds["lon"].values
            }
        )
    except Exception as e:
        print(f"❌ Failed to open {file_url}: {e}")
        return None

# ───────────────────────────────────────────────
# 6. Loop through tiles and write to Zarr
# ───────────────────────────────────────────────
first_written = False
for i, result in enumerate(search_results):
    if i % 100 == 0:
        print(f"📦 Processing tile {i} of {len(search_results)}")

    file_url = next(
        (u["URL"] for u in result["umm"]["RelatedUrls"] if u["URL"].endswith(".nc")),
        None
    )
    if not file_url:
        continue

    ds = open_remote_file(file_url)
    if ds:
        ds = ds.chunk({"tile_id": 100,"time": 1, "lat": 3601, "lon": 3601})
        if not first_written:
            ds.to_zarr(store, mode="w", consolidated=False)
            first_written = True
        else:
            ds.to_zarr(store, mode="a", consolidated=False, append_dim="tile_id")

# ───────────────────────────────────────────────
# 7. Visualise One Tile from Remote Store
# ───────────────────────────────────────────────
ds = xr.open_dataset(zarr_path, engine="zarr", chunks={})
ds.isel(time=0).elevation.plot(cmap="terrain")
plt.title("SRTM 1-Arc Second Elevation")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.show()

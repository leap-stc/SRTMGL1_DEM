import xarray as xr
import fsspec
import earthaccess
import os
from datetime import datetime
import warnings
import numpy as np
import time
import logging
import sys

# ───────────────────────────────────────────────
# Log everything to STRM.log
# ───────────────────────────────────────────────
log_filename = "STRM.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename, mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)

warnings.filterwarnings("ignore")

def open_remote_file(url, i, session):
    """Read one tile, return a 1‐tile Dataset or None."""
    max_attempts, wait = 5, 5
    ds = None
    for attempt in range(1, max_attempts+1):
        try:
            logging.info("→ Attempt %d/%d opening %s", attempt, max_attempts, url)
            with fsspec.open(url, headers=session.headers).open() as f:
                ds = xr.open_dataset(f, engine="h5netcdf")
                ds.load()
            break
        except Exception as e:
            logging.warning("❌ Attempt %d failed: %s", attempt, e)
            if attempt < max_attempts:
                time.sleep(wait)
            else:
                logging.error("⚠️ All attempts failed for %s", url)
                return None

    if ds is None or "SRTMGL1_DEM" not in ds:
        logging.error("Missing data in %s", url)
        return None

    # extract and expand
    dem = ds["SRTMGL1_DEM"].expand_dims(tile_id=[i]).compute()
    lat2d = ds["lat"].values[np.newaxis, :]
    lon2d = ds["lon"].values[np.newaxis, :]
    lat_vals = ds["lat"].values
    lon_vals = ds["lon"].values
    logging.info(
        "Tile %d lat range: %.6f → %.6f", 
        i, lat_vals.min(), lat_vals.max()
    )
    logging.info(
        "Tile %d lon range: %.6f → %.6f", 
        i, lon_vals.min(), lon_vals.max()
    )
    ds_tile = xr.Dataset(
        {
            "elevation": (["tile_id","y","x"], dem.values),
            "lat":       (["tile_id","lat"],  lat2d),
            "lon":       (["tile_id","lon"],  lon2d),
        },
        coords={"tile_id":[i], "time":[datetime(2000,2,11)]}
    )
    return ds_tile

def process_data(zarr_path, product_name):
    # failure log
    failure_log = "failed_tiles.txt"
    open(failure_log, "a").close()

    auth = earthaccess.login(strategy="netrc", persist=True)
    logging.info("Authenticated? %s", auth.authenticated)
    session = earthaccess.get_requests_https_session()

    search_results = earthaccess.search_data(
        short_name="SRTMGL1_NC",
        bounding_box=(-180,-90,180,90)
    )

    first_written = False
    buffer = []  # to hold up to 2 tile‐Datasets

    for i, result in enumerate(search_results):
        file_url = next(
            (u["URL"] for u in result["umm"]["RelatedUrls"]
                       if u["URL"].endswith(".nc")),
            None
        )
        if not file_url:
            continue

        ds_tile = open_remote_file(file_url, i, session)
        if ds_tile is None:
            with open(failure_log, "a") as f:
                f.write(file_url + "\n")
            logging.warning("Recorded failed URL: %s", file_url)
            continue

        buffer.append(ds_tile)

        # once we have two, or at the end, flush them together
        if len(buffer) == 2:
            combined = xr.concat(buffer, dim="tile_id")
            combined = combined.chunk({
                "time":1,
                "tile_id":2,
                "lat":6000,
                "lon":4000
            })

            if not first_written:
                combined.to_zarr(zarr_path, mode="w", consolidated=True)
                first_written = True
            else:
                combined.to_zarr(zarr_path, mode="a", append_dim="tile_id")

            buffer = []

    # flush a final odd tile if present
    if buffer:
        combined = xr.concat(buffer, dim="tile_id")
        combined = combined.chunk({
            "time":1,
            "tile_id":len(buffer),
            "lat":6000,
            "lon":4000
        })
        combined.to_zarr(zarr_path, mode="a", append_dim="tile_id")

if __name__ == "__main__":
    root_dir    = "gs://leap-persistent/data-library"
    product_name= "SRTMGL1_DEM"
    zarr_path   = os.path.join(root_dir, product_name, f"{product_name}.zarr")
    process_data(zarr_path, product_name)

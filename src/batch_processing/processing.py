import gzip
import requests
import time
import json
import os
from datetime import datetime
from random import uniform
from typing import List, Tuple

import rasterio
from rasterio.windows import from_bounds
from rasterio.enums import Resampling
from rasterio.crs import CRS
from rasterio.warp import transform_bounds

from azure.storage.blob import BlobServiceClient

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.utils.batch_task_utils import update_progress_file, cleanup_local_files
from src.utils.azure_storage_utils import upload_blob_to_azure_with_sas

def unzip_file(url: str) -> bytes:
    """
    Opens an object at a given url, and returns a decompressed byte object

    Parameters
    -----------
    url : str
        The base url to the source file
    
    Returns
    -------
    bytes
        Decompressed byte object
    """
    unzipped_file = requests.get(url) 
    if unzipped_file.status_code == 200:
        if ".gz" in url:
            output_file = gzip.decompress(unzipped_file.content)
        else:
            # some files are annoyingly not zipped
            output_file = unzipped_file.content
        return output_file
    else:
        raise Exception(f"Failed to download file from {url}. Status code: {unzipped_file.status_code}")


def clip_to_cog(input_tiff: str, clipped_tiff: str, bbox: list, bbox_crs: str):
    """
    Clips a GeoTIFF to a specified bounding box, handling differing CRS,
    and saves it as a Cloud-Optimized GeoTIFF (COG).

    Args:
        input_tiff: Path to the source GeoTIFF file.
        clipped_tiff: Path for the output clipped COG file.
        bbox: A list representing the bounding box in the format
              [min_x, min_y, max_x, max_y].
        bbox_crs: The Coordinate Reference System of the provided bounding box,
                  defaulting to WGS84 ('EPSG:4326').
    """
    try:
        with rasterio.open(input_tiff) as src:
        
            # Get the CRS of the source raster
            src_crs = src.crs
            
            # Reproject the bounding box if the CRS are different
            if CRS.from_string(bbox_crs) != src_crs:
                left, bottom, right, top = transform_bounds(
                    CRS.from_string(bbox_crs),
                    src_crs,
                    *bbox
                )
                reprojected_bbox = [left, bottom, right, top]
            else:
                reprojected_bbox = bbox
        
        
            window = from_bounds(*reprojected_bbox, src.transform)
            data = src.read(window=window)
            window_transform = src.window_transform(window)

            profile = src.profile.copy()
            profile.update({
                'height': window.height, 
                'width': window.width, 
                'transform': window_transform,
                'tiled': True, 
                'blockxsize': 512, 
                'blockysize': 512,
                'compress': 'deflate'
            })

            # write COG
            with rasterio.open(clipped_tiff, 'w', **profile) as dst:
                dst.write(data)

                factors =  [2, 4, 8, 16]
                dst.build_overviews(factors, Resampling.average)
                dst.update_tags(ns='rio_overview', resampling='average')
    except Exception as e:
        print(f"An error has occurred: {e}")


def decompress_convert_to_cog(work_item: dict, directory: str):
    """
    Download, decompress, and convert a single CHIRPS rainfall data file to Cloud Optimized GeoTIFF (COG) format.
    
    This function processes one rainfall data file by downloading it from a URL, decompressing the .gz file,
    writing it to disk, and then clipping it to Nigeria's bounding box before converting to COG format.
    
    Parameters
    ----------
    work_item : dict
        Dictionary containing file processing information with the following keys:
        - 'url' : str
            Full URL to the .tif.gz file to be downloaded and processed
        - 'year' : str
            Year string (e.g., '1981') used for filename extraction from URL path
    directory : str
        Base directory path where the processed files will be saved. Should end with '/'.
        The function will save the intermediate .tif file in this directory and the final
        COG file in the 'cogs/' subdirectory.
    
    Returns
    -------
    full_path_to_file: str
        Returns the full local path to the file that is processed
    
    Note
    ----
    The Nigeria bounding box coordinates are hardcoded as:
    [2.316388, 3.837669, 15.126447, 14.153350] in EPSG:4326 CRS.
    """
    url = work_item['url']
    year = work_item['year']
    year_dir = str(year) + "/"
    
    # getting file name from url
    raw_file_name = url.split(year_dir)[1].replace(".gz", "")
    decompressed_file = unzip_file(work_item['url'])
    
    # full path of the output tif files
    raw_file_path = os.path.join(directory, "raw-data", raw_file_name)
    
    with open(raw_file_path, "wb") as f:
        f.write(decompressed_file)
    
    cog_file_name = f"nigeria-cog-{raw_file_name}"
    clipped_tiff_path = os.path.join(f"{directory}processed-cogs", cog_file_name)
    bbox_aoi = [2.316388, 3.837669, 15.126447, 14.153350]
    bbox_crs = "EPSG:4326"
    
    clip_to_cog(raw_file_path, clipped_tiff_path, bbox_aoi, bbox_crs)
    
    # return COG file path
    return (clipped_tiff_path, cog_file_name, raw_file_path, raw_file_name)    


def decompress_convert_to_cog_with_retry(work_item: dict, directory: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            decompress_convert_to_cog(work_item, directory)
            return
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = uniform(1, 3) * (2 ** attempt)  # Exponential backoff
                time.sleep(wait_time)
            else:
                raise e
            
    
def process_batch_with_progress(work_items_chunk: List[dict]):

    # Get task ID from environment instead of parameter
    task_id = os.environ.get('AZ_BATCH_TASK_ID', f'local_task_{int(time.time())}')

    # Use Batch VM working directory
    directory = "/tmp/processing/"
    os.makedirs(directory, exist_ok=True)

    # Get Azure storage configuration
    storage_account_url = os.environ["STORAGE_ACCOUNT_URL"]
    cog_sas = os.environ["COG_CONTAINER_SAS"]
    raw_sas = os.environ["RAW_CONTAINER_SAS"]
    logs_sas = os.environ["LOGS_CONTAINER_SAS"]

    # progress reporting vars:
    failed_files = []
    completed = []

    # adding clean up var
    cleanup = []

    processed_count = 0
    for i, item in enumerate(work_items_chunk):
        try:
            cog_container_name = "processed-cogs"
            raw_container_name = "raw-data"
            cog_file_path, cog_file_name, raw_file_path, raw_file_name = decompress_convert_to_cog(item, directory)

            year = item['year']

            upload_blob_to_azure_with_sas(storage_account_url, cog_container_name, cog_file_path, f"{year}/{cog_file_name}", cog_sas)
            upload_blob_to_azure_with_sas(storage_account_url, raw_container_name, raw_file_path, f"{year}/{raw_file_name}", raw_sas)

            completed.append((cog_file_path, raw_file_path)) # progress tracking
            cleanup.append((cog_file_path, raw_file_path)) # cleanup files
        except Exception as e:
            failed_files.append({"item": item, "Error": str(e)})
            print(f"Failed: {item} - Error: {str(e)}")
            continue

        processed_count += 1

        if processed_count % 10 == 0 or i == len(work_items_chunk) - 1:
            print(f"Task ID: {task_id}, Completed: {completed}, Failed Files: {failed_files}")
            # Use the utility function with proper parameters
            update_progress_file(
                task_id=task_id,
                completed=completed,
                failed=failed_files,
                total=len(work_items_chunk),
                storage_account_url=storage_account_url,
                logs_sas=logs_sas,
                progress_file_prefix="task_"
            )
            if cleanup:
                cleanup_local_files(cleanup)
                cleanup.clear()
            
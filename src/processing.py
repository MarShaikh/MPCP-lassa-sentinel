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
from azure.identity import DefaultAzureCredential

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
        decompressed_file = gzip.decompress(unzipped_file.content)
    
    return decompressed_file


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
            
    
def update_progress_file(task_id, completed, failed_files):  # Write to progress/task_{id}.json  
    """
    Updates a progress file called {task_id}.json and uploads it to logs on Azure Blob Store

    Parameters:
    -----------
    task_id: str
        Batch number 
    completed: str
        The number of files that were processed in this batch
    failed_files: dict
        A dict of failed files that could have failed due to various reasons

    Returns
    -------
    None
    """
    iso_timestamp = datetime.now().isoformat()
    batch_number = task_id
    completed = completed
    
    json_file = {
        "iso_timestamp": iso_timestamp,
        "batch_number": batch_number,
        "completed": completed,
        "failed_files": failed_files
    }
    
    local_path = "../data/nigeria_tifs/batch-logs"
    file_name = f"{task_id}.json"
    upload_file_path = os.path.join(local_path, file_name)
    container_name = "batch-logs"

    with open(upload_file_path, 'w') as f:
        json.dump(json_file, f)

    upload_blob_to_azure(container_name=container_name, file_path=upload_file_path, file_name=file_name)
    cleanup_local_files(upload_file_path)

    
    
def upload_blob_to_azure(container_name: str, file_path: str, file_name: str):
    """
    Uploads a local file at <file_path> to a blob names <file_name> within a container <container_name>

    Parameters:
    ----------
    container_name: str
        Name of the container on Azure Blob Storage account
    
    file_path: str
        Path to the local file

    file_name: str
        Name of the uploaded file in the container

    Returns:
    -------
    None
    """
    
    # uses the default credential option on this machine
    credential = DefaultAzureCredential()

    # Create blob service client
    blob_service_client = BlobServiceClient(
        account_url="https://mpcpstorageaccount.blob.core.windows.net",
        credential=credential
    )
    
    blob_client = blob_service_client.get_blob_client(container = container_name, blob=file_name)
    
    print(f"\nUploading to Azure as blob:\n\t" + file_path)
    with open(file = file_path, mode = "rb") as data:
        blob_client.upload_blob(data)    

def cleanup_local_files(file_paths: List[Tuple] | str):  # Delete local files after uploading them to Azure Blob
    try:
        if type(file_paths) == str:
            os.remove(file_paths)
            print(f"Local {file_paths} removed")
        else:
            for (i, j) in file_paths:
                os.remove(i) # processed file
                os.remove(j) # raw file
                print(f"Local raw file removed: {i} and COG file: {j} removed.")
    except FileNotFoundError:
        print(f"File '{i}' not found.")
        print(f"File '{j}' not found.")
    

def process_batch_with_progress(work_items_chunk: List[dict], task_id: int):
    failed_files = []
    completed = []
    directory = "../data/nigeria_tifs/" # hard coding this for local run
    
    processed_count = 0
    for i, item in enumerate(work_items_chunk):
        try: 
            cog_container_name = "processed-cogs"
            raw_container_name = "raw-data"
            cog_file_path, cog_file_name, raw_file_path, raw_file_name = decompress_convert_to_cog(item, directory)

            year = item['year']

            upload_blob_to_azure(container_name=cog_container_name, file_path=cog_file_path, file_name=f"{year}/{cog_file_name}")
            upload_blob_to_azure(container_name=raw_container_name, file_path=raw_file_path, file_name=f"{year}/{raw_file_name}")

            completed.append((cog_file_path, raw_file_path))
        except Exception as e:
            failed_files.append({"item": item, "Error": str(e)})
            print(f"Failed: {item} - Error: {str(e)}")
            continue

        processed_count += 1
        
        if processed_count % 10 == 0 or i == len(work_items_chunk) - 1:
            print(f"Task ID: {task_id}, Completed: {completed}, Failed Files: {failed_files}")
            update_progress_file(task_id, len(completed), failed_files)
            cleanup_local_files(completed)
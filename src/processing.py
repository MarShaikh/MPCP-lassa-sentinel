import gzip
import requests
import time
from random import uniform

import rasterio
from rasterio.windows import from_bounds
from rasterio.enums import Resampling
from rasterio.crs import CRS
from rasterio.warp import transform_bounds


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
    None
        This function does not return any value. It performs file I/O operations and
        creates processed files on disk.
    
    Note
    ----
    The Nigeria bounding box coordinates are hardcoded as:
    [2.316388, 3.837669, 15.126447, 14.153350] in EPSG:4326 CRS.
    """
    url = work_item['url']
    year = work_item['year']
    year_dir = str(year) + "/"
    
    # getting file name from url
    file_name = url.split(year_dir)[1].replace(".gz", "")
    decompressed_file = unzip_file(work_item['url'])
    
    # full path of the output tif files
    full_path_to_file = directory + "/raw/" + file_name
    
    with open(full_path_to_file, "wb") as f:
        f.write(decompressed_file)
    
    # change this to adapt it to other locations
    clipped_tiff = f"{directory}cogs/" + f"nigeria-cog-{file_name}" 
    bbox_aoi = [2.316388, 3.837669, 15.126447, 14.153350]
    bbox_crs = "EPSG:4326"
    clip_to_cog(full_path_to_file, clipped_tiff, bbox_aoi, bbox_crs)    


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
"""
Module for converting COG files to STAC items and storing them in Azure Blob Storage.
"""
import os
import json
from datetime import datetime, timezone
from typing import Dict, Optional

import pystac
from rio_stac import create_stac_item
from rasterio.io import MemoryFile
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.identity import DefaultAzureCredential

def extract_metadata_from_filename_chirps(filename: str) -> Optional[Dict]:
    """
    Extracts key metadata from a CHIRPS filename using regular expressions.
    The filename is of the format: nigeria-cog-chirps-v2.0.1981.01.01

    Args:
        filename (str): The filename to parse.

    Returns:
        dict: A dictionary containing the extracted metadata.
    """
    try:
        parsed_list = filename.split("-")
        product_type = parsed_list[2]
        version_year_list = parsed_list[3].split(".")
        version = '.'.join(version_year_list[0:2])
        date = datetime.strptime("-".join(version_year_list[2:]), '%Y-%m-%d')
        date = date.replace(tzinfo=timezone.utc) 
        creation_date = datetime.now(timezone.utc)

        # create a dictionary to organise all of the extracted metadata:
        metadata = {
            "product_type": product_type,
            "version": version,
            "date": date,
            "creation_date": creation_date
        }
        return metadata
    except Exception as e:
        print("Failed to extract metadata due to {e}")
        return None
    

def create_stac_item_from_cog_chirps(blob_client, filename: str, blob_domain: str, 
                               container_name: str, year: str) -> pystac.Item:
    """
    Create a STAC Item for CHIRPS data using rio-stac with proper spatial handling.
    
    Args:
        blob_client: Azure blob client for the COG file
        filename (str): Name of the COG file
        blob_domain (str): Azure storage account URL
        container_name (str): Name of the container
        year (str): Year string for the data
        
    Returns:
        pystac.Item: STAC Item with metadata
    """
    blob_data = blob_client.download_blob().readall()
    blob_properties = blob_client.get_blob_properties()
    blob_size = blob_properties.size
    item_id = filename.replace('.tif', '')
    
    try:
        with MemoryFile(blob_data) as memfile:
            with memfile.open() as dataset:
                # Create base STAC item from rasterio dataset
                stac_item = create_stac_item(
                    source=dataset,
                    id=item_id,
                    asset_name='data',
                    asset_href=f"{blob_domain}/{container_name}/{year}/{filename}",
                    with_proj=True,
                    with_raster=True,
                    properties={
                        'datetime': None,
                        'raster:bands': [
                            {
                                'nodata': dataset.nodata, # Value representing no data in the raster
                                'data_type': dataset.dtypes[0], # Data type of the raster (e.g., uint16)
                                'spatial_resolution': dataset.res[0] # Spatial resolution of the raster in meters
                            }
                        ],
                        'file:size': blob_size # Size of the file in bytes
                    },
                    extensions=[
                        'https://stac-extensions.github.io/file/v2.1.0/schema.json' # Add the file extension schema for additional metadata
                    ]
                )
                return stac_item
    except Exception as e:
        raise Exception(f"Failed to create STAC item: {e}")

def enhance_stac_item_with_metadata_chirps(stac_item: pystac.Item, metadata_from_filename: Dict) -> pystac.Item:
    """
    Enhances a STAC Item with additional metadata from CHIRPS filename.
    
    Args:
        stac_item (pystac.Item): Existing STAC Item created by rio-stac
        metadata_from_filename (dict): Metadata extracted from filename
        
    Returns:
        pystac.Item: Enhanced STAC Item
    """
    stac_item.datetime = None
    
    stac_item.common_metadata.start_datetime = metadata_from_filename['date']
    stac_item.common_metadata.end_datetime = metadata_from_filename['date']
    stac_item.common_metadata.created = metadata_from_filename['creation_date']
    
    stac_item.properties.update({
        'product_type': metadata_from_filename['product_type'],
        'version': metadata_from_filename['version']
    })
    
    return stac_item

def process_cog_to_stac(blob_client, filename: str, blob_domain: str, container_name: str, year: str) -> pystac.Item:
    """
    Process a single COG file to create a STAC item.
    
    Args:
        blob_client: Azure blob client for the COG file
        filename (str): Name of the COG file
        blob_domain (str): Azure storage account URL
        container_name (str): Name of the container
        year (str): Year string for the data
        
    Returns:
        dict: The STAC item as a dictionary
    """
    # Extract metadata from filename
    file_name = filename.split(".tif")[0]
    metadata = extract_metadata_from_filename_chirps(file_name)

    if metadata is None:
        raise ValueError(f"Could not extract metadata from filename: {file_name}")
    
    # create STAC item
    stac_item = create_stac_item_from_cog_chirps(
        blob_client, filename, blob_domain, container_name, year
    )

    # enhance with metadata
    stac_item = enhance_stac_item_with_metadata_chirps(stac_item, metadata)

    return stac_item.to_dict()

def save_stac_item_to_blob(stac_item_dict: Dict, blob_service_client: BlobServiceClient, 
                           container_name: str, blob_path: str):
    """
    Save a STAC item JSON to Azure Blob Storage.
    
    Args:
        stac_item_dict (dict): STAC item as dictionary
        blob_service_client: Azure blob service client
        container_name (str): Target container name
        blob_path (str): Path for the blob (e.g., "stac-items/1981/item.json")
    """
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_path
    )
    
    stac_json = json.dumps(stac_item_dict, indent=2)
    blob_client.upload_blob(stac_json.encode('utf-8'), overwrite=True)
    print(f"Uploaded STAC item to {container_name}/{blob_path}")
    
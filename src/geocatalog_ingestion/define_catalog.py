"""
Create a STAC catalog from existing STAC items in Azure Blob Storage.

This script scans a blob storage container for STAC item JSON files and creates
a catalog.json file that references all items. The catalog is then uploaded back
to the same container.

IMPORTANT - PARAMETERS TO CUSTOMIZE FOR YOUR USE CASE:
=========================================================

1. CATALOG METADATA (lines 42-45):
   - CATALOG_ID: Unique identifier for your catalog
   - CATALOG_TITLE: Human-readable catalog title
   - CATALOG_DESCRIPTION: Detailed description of the catalog contents
   - STAC_VERSION: STAC specification version (usually "1.0.0")

2. BLOB FILTER PATTERN (line 69):
   - Change the pattern in the if statement to match YOUR STAC item file naming convention
   - Current: 'nigeria-cog-chirps-v2.0.' in blob.name
   - Example for different pattern: 'ghana-modis-' in blob.name
   - This determines which JSON files are included in the catalog

3. LINK METADATA (line 73):
   - "title": Change to describe your specific items
   - Current: "Nigeria CHIRPS item"
   - Example: "Ghana MODIS item"

4. OUTPUT FILENAME (line 48):
   - OUTPUT_FILE: Name for the catalog file (typically "catalog.json")
   - This file will be uploaded to the root of your CONTAINER_NAME

ENVIRONMENT VARIABLES REQUIRED:
================================
- STORAGE_ACCOUNT_URL: Azure storage account URL
- CONTAINER_NAME: Container with your STAC items

Example:
    export STORAGE_ACCOUNT_URL="https://mystorageaccount.blob.core.windows.net"
    export CONTAINER_NAME="stac-items"
    python define_catalog.py
"""

import json
import os
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient

# Configuration - Retrieved from environment variables
STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
STORAGE_ACCOUNT_NAME = STORAGE_ACCOUNT_URL.split("//")[1].split(".")[0]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]
ACCOUNT_URL = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

# Catalog metadata - CUSTOMIZE THESE FOR YOUR USE CASE
CATALOG_ID = "nigeria-chirps-catalog"
CATALOG_TITLE = "Nigeria CHIRPS Data Catalog"
CATALOG_DESCRIPTION = "CHIRPS v2.0 precipitation data for Nigeria (1981-2025)"
STAC_VERSION = "1.0.0"

# Output file - Name of the catalog JSON file to create
OUTPUT_FILE = "catalog.json"

print("Connecting to Azure Blob Storage...")

# Authenticate using Azure CLI credentials
credential = AzureCliCredential()
blob_service_client = BlobServiceClient(
    account_url=ACCOUNT_URL,
    credential=credential
)

# Get container client
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

print(f"Listing blobs in container '{CONTAINER_NAME}'...")

# List all blobs and filter for STAC item JSON files
links = []
blob_count = 0

for blob in container_client.list_blobs():
    # Filter for JSON files matching the pattern
    if blob.name.endswith('.json') and 'nigeria-cog-chirps-v2.0.' in blob.name:
        blob_url = f"{ACCOUNT_URL}/{CONTAINER_NAME}/{blob.name}"
        
        link = {
            "rel": "item",
            "href": blob_url,
            "title": "Nigeria CHIRPS item",
            "type": "application/json"
        }
        links.append(link)
        blob_count += 1
        
        # Progress indicator
        if blob_count % 1000 == 0:
            print(f"  Processed {blob_count} items...")

print(f"\nFound {blob_count} STAC item JSON files")

# Build the catalog
catalog = {
    "type": "Catalog",
    "id": CATALOG_ID,
    "title": CATALOG_TITLE,
    "description": CATALOG_DESCRIPTION,
    "stac_version": STAC_VERSION,
    "links": links
}

# Write catalog to file
print(f"\nWriting catalog to Azure Blob store...")

container_name = CONTAINER_NAME
blob_path = OUTPUT_FILE

blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_path
    )
catalog = json.dumps(catalog, indent=2)
blob_client.upload_blob(catalog.encode('utf-8'), overwrite=True)
print(f"Uploaded STAC catalog JSON to {container_name}/{blob_path}")
# code to build the `catalog.json` file and save it to blob storage under `stac-items/catalog.json`
import json
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient

# Configuration
STORAGE_ACCOUNT_NAME = "mpcpstorageaccount"
CONTAINER_NAME = "stac-items"
ACCOUNT_URL = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

# Catalog metadata
CATALOG_ID = "nigeria-chirps-catalog"
CATALOG_TITLE = "Nigeria CHIRPS Data Catalog"
CATALOG_DESCRIPTION = "CHIRPS v2.0 precipitation data for Nigeria (1981-2025)"
STAC_VERSION = "1.0.0"

# Output file
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
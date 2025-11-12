# Microsoft Planetary Computer Pipelines - Setup Documentation

This guide provides step-by-step instructions for setting up the Microsoft Planetary Computer Pipelines on Azure.

## Important: Two Workflows with Different Requirements

This project supports two distinct workflows:

1. **Workflow 1: Direct Ingestion from Planetary Computer**
   - **Requires:** GeoCatalog only
   - **Follow:** Prerequisites, Azure Account Setup, GeoCatalog Setup, Data Ingestion

2. **Workflow 2: CHIRPS Data Processing Pipeline (Complex)**
   - **Requires:** All Azure resources (GeoCatalog, Storage, Batch)
   - **See also:** [BATCH_AND_PROCESSING.md](./BATCH_AND_PROCESSING.md) for batch setup details

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Azure Account Setup](#azure-account-setup)
3. [GeoCatalog Setup](#geocatalog-setup)
4. [Storage Account Configuration](#storage-account-configuration)
5. [Creating STAC Collections](#creating-stac-collections)
6. [Using the Explorer](#using-the-explorer)
7. [Data Ingestion](#data-ingestion)
8. [Authentication Configuration](#authentication-configuration)
9. [Next Steps](#next-steps)

---

## Prerequisites

- Azure subscription with appropriate permissions
- Azure CLI installed and configured
- Python 3.11+
- Conda environment manager
- Git

### Install Azure CLI

```bash
# macOS
brew update && brew install azure-cli

# Linux
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Windows
# Download from https://aka.ms/installazurecliwindows
```

### Conda Environment Setup

```bash
# Create conda environment
conda create -n MPCP_lassasentinel python=3.11 -y
conda activate MPCP_lassasentinel

# Install dependencies
pip install -r requirements.txt
```

---

## Azure Account Setup

### 1. Login to Azure

```bash
az login
```

### 2. Set Your Subscription

```bash
# Set your subscription ID
export SUBSCRIPTION_ID=<subscription_id>
az account set --subscription "$SUBSCRIPTION_ID"
```

### 3. Register Microsoft.Orbital Resource Provider

This is required for GeoCatalog services:

```bash
az provider show -n Microsoft.Orbital
az provider register --namespace Microsoft.Orbital
```

### 4. Create Resource Group

```bash
export RESOURCE_GROUP=<resource_group>
export LOCATION=<location>

az group create \
    --name $RESOURCE_GROUP \
    --location $LOCATION
```

**Note:** GeoCatalog is only available in specific regions: `canadacentral`, `northcentralus`, and `westeurope`.

---

## GeoCatalog Setup

### 1. Define GeoCatalog Parameters

```bash
export CATALOG_NAME=<geocatalog_name>
```

**Important:** Catalog names must match the pattern `^[a-zA-Z0-9-]{3,24}$` (alphanumeric and hyphens only, 3-24 characters).

### 2. Create GeoCatalog Instance

```bash
az rest --method PUT \
    --uri "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Orbital/geoCatalogs/$CATALOG_NAME?api-version=2025-02-11-preview" \
    --body '{
        "location": "'$LOCATION'",
        "Properties": {
            "tier": "Basic"
        }
    }'
```

This process takes approximately 10-20 minutes.

### 3. Monitor Deployment Status

```bash
az rest --method GET \
    --uri "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Orbital/geoCatalogs/$CATALOG_NAME?api-version=2025-02-11-preview"
```

Wait until `provisioningState` shows `"Succeeded"`.

### 4. Retrieve Catalog URI

```bash
az resource show \
    -g $RESOURCE_GROUP \
    -n $CATALOG_NAME \
    --namespace Microsoft.Orbital \
    --resource-type "geocatalogs"
```

Save the `catalogUri` from the output.

---

## Storage Account Configuration

> **Note:** This section is only required for **Workflow 2 (CHIRPS Pipeline)**. If you're only using Workflow 1 (Planetary Computer ingestion), you can skip to [Data Ingestion](#data-ingestion).

### 1. Create Storage Account

```bash
export STORAGE_ACCOUNT_NAME="mpcpstorageaccount"

az storage account create \
    --name $STORAGE_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku Standard_LRS \
    --kind StorageV2
```

### 2. Create Storage Containers

In my implementation, I used five containers: `processed-cogs`, `raw-data`, `batch-logs`, `stac-items`, `task-data`.

```bash
# Authenticate with Azure AD
az storage container create \
    --name <container_name> \
    --account-name $STORAGE_ACCOUNT_NAME \
    --auth-mode login
```

Repeat this command for each container you need.

### 3. Configure Lifecycle Management (Optional)

Create a lifecycle policy file to automatically manage old data:

```bash
az storage account management-policy create \
    --account-name $STORAGE_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --policy @mpcpstorageaccount-lifecycle-policy.json
```

Example `mpcpstorageaccount-lifecycle-policy.json`:

```json
{
  "rules": [
    {
      "enabled": true,
      "name": "delete-old-raw-data",
      "type": "Lifecycle",
      "definition": {
        "actions": {
          "baseBlob": {
            "delete": {
              "daysAfterModificationGreaterThan": 90
            }
          }
        },
        "filters": {
          "blobTypes": ["blockBlob"],
          "prefixMatch": ["raw-data/"]
        }
      }
    }
  ]
}
```

---

## Creating STAC Collections

STAC (SpatioTemporal Asset Catalog) collections organize geospatial data with metadata.

> **Note:** This is a sample setup code for creating a collection on **Microsoft Planetary Computer Pro**

### 1. Fetch Collection from Planetary Computer

```python
import requests
import azure.identity

# Example: Using io-lulc-annual-v02 collection
collection_name = "io-lulc-annual-v02"
collection = requests.get(
    f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{collection_name}"
).json()

# Remove collection-level assets (not supported in GeoCatalog)
collection.pop("assets", None)

# Customize collection metadata
collection["id"] = "mpc-quickstart"
collection["title"] += " (Planetary Computer Quickstart)"
```

### 2. Authenticate with GeoCatalog

```python
credential = azure.identity.DefaultAzureCredential()
token = credential.get_token("https://geocatalog.spatio.azure.com")
headers = {"Authorization": f"Bearer {token.token}"}

geocatalog_url = "https://geospatialdm.fmd9dgfcd2fab5hw.westeurope.geocatalog.spatio.azure.com"
```

### 3. Create Collection in GeoCatalog

```python
response = requests.post(
    f"{geocatalog_url}/stac/collections",
    json=collection,
    headers=headers,
    params={"api-version": "2025-04-30-preview"}
)

print(f"Status: {response.status_code}")
print(f"Collection URL: {geocatalog_url}/collections/{collection['id']}")
```

---

## Using the Explorer

**Critical Requirement for Explorer Visibility:**
For the GeoCatalog Explorer tab to appear and allow data visualization, collections **must** include:
1. The [Item Assets Extension](https://github.com/stac-extensions/item-assets)
2. At least one `item_assets` entry with a visualizable media type (e.g., `image/tiff; application=geotiff; profile=cloud-optimized`)

Without `item_assets` in the collection metadata, the Explorer option will not be available even if items are successfully ingested.

### 1. Configure Render Options

Define how items are visualized in the Explorer:

```python
render_option = {
    "id": "default",
    "name": "Default",
    "description": "Land cover classification using custom colormap",
    "type": "raster-tile",
    "options": "assets=data&colormap_name=viridis",
    "minZoom": 6
}

response = requests.post(
    f"{geocatalog_url}/stac/collections/{collection['id']}/configurations/render-options",
    json=render_option,
    headers=headers,
    params={"api-version": "2025-04-30-preview"}
)
```

### 2. Define Mosaic Configuration

Control how items are queried and combined:

```python
mosaic = {
    "id": "most-recent",
    "name": "Most recent available",
    "description": "Show the most recent available data",
    "cql": []
}

response = requests.post(
    f"{geocatalog_url}/stac/collections/{collection['id']}/configurations/mosaics",
    json=mosaic,
    headers=headers,
    params={"api-version": "2025-04-30-preview"}
)
```

### 3. Set Tile Settings

```python
tile_settings = {
    "minZoom": 6,
    "maxItemsPerTile": 35
}

response = requests.put(
    f"{geocatalog_url}/stac/collections/{collection['id']}/configurations/tile-settings",
    json=tile_settings,
    headers=headers,
    params={"api-version": "2025-04-30-preview"}
)
```

---

## Data Ingestion

### Ingestion from Microsoft Planetary Computer

The project includes a script to ingest data from Microsoft Planetary Computer to your Azure GeoCatalog.

#### Basic Usage

```bash
python src/ingestion/ingestion_from_datacatalog.py \
    --geocatalog-url "https://geospatialdm.fmd9dgfcd2fab5hw.westeurope.geocatalog.spatio.azure.com" \
    --pc-collection "modis-13Q1-061" \
    --bbox-aoi 2.316388 3.837669 15.126447 14.153350 \
    --date-range "2020-01-01/2020-12-31" \
    --region "nigeria" \
    --batch-size 100 \
    --api-version "2025-04-30-preview" \
    --mpc-app-id "https://geocatalog.spatio.azure.com"
```

#### Parameters

- `--geocatalog-url`: Your GeoCatalog URI
- `--pc-collection`: Collection ID from Planetary Computer
- `--bbox-aoi`: Bounding box (min_lon min_lat max_lon max_lat)
- `--date-range`: Date range in ISO format (start/end)
- `--region`: Region name for collection ID
- `--batch-size`: Number of items per ingestion batch
- `--api-version`: GeoCatalog API version
- `--mpc-app-id`: Microsoft Planetary Computer app ID

#### Finding Valid Collections

Browse available collections at: https://planetarycomputer.microsoft.com/catalog

---

## Authentication Configuration

> **Note:** This section is only required for **Workflow 2 (CHIRPS Pipeline)**. If you're only using Workflow 1 (Planetary Computer ingestion), you can skip to [Next Steps](#next-steps).

### 1. Create Service Principal

```bash
az ad sp create-for-rbac --name "mpcp-batch-sp" --role contributor --scopes /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP
```

Save the output:

```json
{
  "appId": "XXXXXX",
  "password": "XXXXXX",
  "tenant": "XXXXXX"
}
```

### 2. Grant Service Principal Permissions

```bash
export SERVICE_PRINCIPAL_ID="<appId-from-previous-step>"

# Batch account access
az role assignment create \
    --assignee $SERVICE_PRINCIPAL_ID \
    --role "Contributor" \
    --scope /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Batch/batchAccounts/$BATCH_ACCOUNT_NAME

# Storage account access
az role assignment create \
    --assignee $SERVICE_PRINCIPAL_ID \
    --role "Storage Blob Data Contributor" \
    --scope /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT_NAME
```

### 3. Configure Environment Variables

Create `configs/config.env`:

```bash
# Azure Batch Configuration
export AZURE_TENANT_ID="XXXXXX"
export AZURE_CLIENT_ID="XXXXXX"
export AZURE_CLIENT_SECRET="XXXXXX"
export BATCH_ACCOUNT_URL="https://mpcpbatch.westeurope.batch.azure.com"

# Azure Storage Configuration
export STORAGE_ACCOUNT_URL="https://mpcpstorageaccount.blob.core.windows.net"
export BATCH_STORAGE_ACCOUNT_KEY="<your-storage-account-key>"

# Container Names
export CONTAINER_NAME="stac-items"
```

Get the storage account key:

```bash
az storage account keys list \
    --account-name $STORAGE_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --query "[0].value" -o tsv
```

Load environment variables:

```bash
source configs/config.env
```

---

## Next Steps

Now that you've completed the basic setup, you can proceed with:

1. **[Batch Setup and Processing Pipelines](./BATCH_AND_PROCESSING.md)** - Configure Azure Batch, set up ingestion sources, and run processing pipelines
2. **[Testing](./TESTING.md)** - Run unit and integration tests
3. **[Troubleshooting](./TROUBLESHOOTING.md)** - Common issues and solutions

---

## Additional Resources

### Documentation

- [Microsoft Planetary Computer Docs](https://learn.microsoft.com/en-us/azure/planetary-computer/)
- [STAC Specification](https://stacspec.org/)
- [Azure Batch Documentation](https://learn.microsoft.com/en-us/azure/batch/)
- [Azure Storage Documentation](https://learn.microsoft.com/en-us/azure/storage/)

### Project-Specific Guides

- [README.md](../README.md) - Quick start and overview
- [BATCH_AND_PROCESSING.md](./BATCH_AND_PROCESSING.md) - Batch setup and processing pipelines
- [TESTING.md](./TESTING.md) - Testing guide
- [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) - Troubleshooting guide

### Useful Commands

```bash
# Check Azure CLI version
az --version

# List all resource groups
az group list --output table

# View storage account details
az storage account show \
    --name $STORAGE_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP

# List blobs in container
az storage blob list \
    --account-name $STORAGE_ACCOUNT_NAME \
    --container-name processed-cogs \
    --auth-mode login \
    --output table
```

---

## Support

For issues or questions:

1. Check the [Troubleshooting Guide](./TROUBLESHOOTING.md)
2. Consult Azure documentation links above
3. Check Azure service health: https://status.azure.com/

---

**Last Updated:** November 2025
**Project:** MPCP Lassa Sentinel Geospatial Data Processing Pipeline

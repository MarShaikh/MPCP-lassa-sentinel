# Batch Setup and Processing Pipelines

This guide covers Azure Batch configuration, ingestion source setup, and running the processing pipelines for the Microsoft Planetary Computer Pipelines project.

> **Important:** This guide is **only for Workflow 2 (CHIRPS Data Processing Pipeline)**. If you're using Workflow 1 (Direct Ingestion from Planetary Computer), you don't need Azure Batch or the setup described in this document. Refer to [SETUP.md](./SETUP.md) for simple ingestion from Planetary Computer.

## Table of Contents

1. [Azure Batch Setup](#azure-batch-setup)
2. [Setting Up Ingestion Sources](#setting-up-ingestion-sources)
3. [Running Processing Pipelines](#running-processing-pipelines)

---

## Azure Batch Setup

### 1. Register Microsoft.Batch Resource Provider

```bash
az provider show --namespace Microsoft.Batch --query "registrationState" -o tsv

# If not registered:
az provider register --namespace Microsoft.Batch
```

### 2. Create Key Vault

```bash
export KEYVAULT_NAME="kvmpcp"

az keyvault create \
    --name $KEYVAULT_NAME \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION
```

### 3. Assign Azure Batch Orchestration Role

```bash
# Note: ddbf3205-c6bd-46ae-8127-60eb93363864 is the fixed service principal ID
# for "Microsoft Azure Batch" across all Azure tenants
az role assignment create \
    --assignee ddbf3205-c6bd-46ae-8127-60eb93363864 \
    --role "Azure Batch Service Orchestration Role" \
    --scope /subscriptions/$SUBSCRIPTION_ID

az role assignment create \
    --assignee ddbf3205-c6bd-46ae-8127-60eb93363864 \
    --role "Key Vault Secrets Officer" \
    --scope /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.KeyVault/vaults/$KEYVAULT_NAME
```

### 4. Create Batch Account (User Subscription Mode)

```bash
export BATCH_ACCOUNT_NAME="mpcpbatch"

az batch account create \
    --name $BATCH_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --storage-account $STORAGE_ACCOUNT_NAME \
    --keyvault $KEYVAULT_NAME
```

**Important:** Creating the account with `--keyvault` parameter automatically configures it in "User Subscription" mode, which is required for Spot VMs.

### 5. Enable Managed Identity for Batch Account

```bash
az batch account identity assign \
    --name $BATCH_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --system-assigned
```

Save the `principalId` from the output.

### 6. Grant Batch Account Access to Storage

```bash
export BATCH_PRINCIPAL_ID="<principal-id-from-previous-step>"

az role assignment create \
    --assignee $BATCH_PRINCIPAL_ID \
    --role "Storage Blob Data Contributor" \
    --scope /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT_NAME
```

### 7. Create Batch Pool

```bash
az batch account set --name $BATCH_ACCOUNT_NAME --resource-group $RESOURCE_GROUP
az batch account login --name $BATCH_ACCOUNT_NAME --resource-group $RESOURCE_GROUP

az batch pool create \
    --id geospatial-processing-pool \
    --vm-size Standard_D2s_v3 \
    --target-dedicated-nodes 3 \
    --image canonical:0001-com-ubuntu-server-jammy:22_04-lts \
    --node-agent-sku-id "batch.node.ubuntu 22.04" \
    --start-task-command-line "/bin/bash -c 'apt-get update && apt-get install -y software-properties-common git && add-apt-repository -y ppa:deadsnakes/ppa && apt-get update && apt-get install -y python3.11 python3.11-pip python3.11-venv python3.11-dev && python3.11 -m pip install --upgrade pip'" \
    --start-task-wait-for-success
```

**Performance Optimization:** For better resource utilization, you can configure multiple tasks per node:

```bash
az batch pool create \
    --id geospatial-processing-pool \
    --vm-size Standard_D2s_v3 \
    --target-dedicated-nodes 3 \
    --task-slots-per-node 4 \
    --image canonical:0001-com-ubuntu-server-jammy:22_04-lts \
    --node-agent-sku-id "batch.node.ubuntu 22.04" \
    --start-task-command-line "/bin/bash -c 'apt-get update && apt-get install -y software-properties-common git && add-apt-repository -y ppa:deadsnakes/ppa && apt-get update && apt-get install -y python3.11 python3.11-pip python3.11-venv python3.11-dev && python3.11 -m pip install --upgrade pip'" \
    --start-task-wait-for-success
```

The `--task-slots-per-node 4` parameter allows up to 4 tasks to run concurrently on each node, improving throughput for I/O-bound tasks.

### 8. Enable Autoscaling (Optional)

```bash
az batch pool autoscale enable \
    --pool-id geospatial-processing-pool \
    --auto-scale-formula '$totalTasks = $PendingTasks.GetSample(1); $activeTasks = $ActiveTasks.GetSample(1); $TargetDedicatedNodes = min(5, $totalTasks + $activeTasks); $NodeDeallocationOption = taskcompletion;' \
    --auto-scale-evaluation-interval "PT5M"
```

---

## Setting Up Ingestion Sources

> **Note:** This section configures GeoCatalog to access data from **your own Azure Blob Storage** (for Workflow 2 - CHIRPS Pipeline). This is NOT needed for Workflow 1 (Planetary Computer ingestion), which reads directly from Microsoft's storage.

GeoCatalog needs ingestion sources to access data from Azure Blob Storage. You need two ingestion sources with specific permissions to enable the CHIRPS processing pipeline to work with your GeoCatalog instance.

### 1. Create Managed Identity for GeoCatalog

Follow the guide: https://learn.microsoft.com/en-us/azure/planetary-computer/set-up-ingestion-credentials-managed-identity

**Key Steps:**
1. Create user-assigned managed identity (if not already done in Azure Maps setup)
2. Assign the identity to your GeoCatalog instance
3. Grant the identity access to storage containers

```bash
# If not created during Azure Maps setup:
export IDENTITY_NAME="${CATALOG_NAME}-id"

az identity create \
    --name $IDENTITY_NAME \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION

# Get identity details
IDENTITY_ID=$(az identity show -n $IDENTITY_NAME -g $RESOURCE_GROUP --query id --output tsv)
IDENTITY_PRINCIPAL_ID=$(az identity show -n $IDENTITY_NAME -g $RESOURCE_GROUP --query principalId --output tsv)

# Grant storage access
az role assignment create \
    --assignee $IDENTITY_PRINCIPAL_ID \
    --role "Storage Blob Data Reader" \
    --scope /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT_NAME
```

**Important:** Role assignments can take 5-10 minutes to propagate. Wait before testing ingestion.

### 2. Configure Ingestion Sources

You need two ingestion sources with different permissions:

**Source 1: STAC Items Source** (for catalog.json and STAC item JSONs)
- Container: `stac-items`
- Permissions needed: Read, List
- Purpose: GeoCatalog reads STAC metadata from this container

**Source 2: COG Files Source** (for actual raster data)
- Container: `processed-cogs`
- Permissions needed: Read, List
- Purpose: GeoCatalog accesses COG raster files

#### Via GeoCatalog GUI

1. Navigate to your GeoCatalog instance in Azure Portal
2. Go to "Ingestion Sources" section
3. Click "Add Ingestion Source"
4. For each source:
   - Name: `stac-items-source` or `cogs-source`
   - Storage Account URL: `https://mpcpstorageaccount.blob.core.windows.net`
   - Container Name: `stac-items` or `processed-cogs`
   - Authentication: Select your managed identity
   - Permissions: Read, List

#### Via Planetary Computer API

You can also configure ingestion sources programmatically using the GeoCatalog REST API. This is particularly useful for automation or CI/CD pipelines.

**API Endpoint:**
```
POST https://{your-catalog}.geocatalog.spatio.azure.com/inma/ingestion-sources?api-version=2025-04-30-preview
```

**Authentication:**
- OAuth2 (Azure AD)
- Scope: `https://geocatalog.spatio.azure.com/.default`

**Two Types of Ingestion Sources:**

1. **Managed Identity (Recommended for production)**
2. **SAS Token (Simpler for testing)**

##### Option 1: Managed Identity Ingestion Source

```python
import requests
import azure.identity
import uuid

# Authenticate
credential = azure.identity.DefaultAzureCredential()
token = credential.get_token("https://geocatalog.spatio.azure.com")
headers = {"Authorization": f"Bearer {token.token}"}

geocatalog_url = "https://your-catalog.westeurope.geocatalog.spatio.azure.com"

# Define managed identity ingestion source
ingestion_source = {
    "id": str(uuid.uuid4()),  # Generate unique ID
    "kind": "BlobManagedIdentity",
    "connectionInfo": {
        "containerUrl": "https://mpcpstorageaccount.blob.core.windows.net/stac-items",
        "objectId": "your-managed-identity-object-id"  # From Azure Portal
    }
}

# Create ingestion source
response = requests.post(
    f"{geocatalog_url}/inma/ingestion-sources",
    json=ingestion_source,
    headers=headers,
    params={"api-version": "2025-04-30-preview"}
)

print(f"Status: {response.status_code}")
if response.status_code == 201:
    print(f"Ingestion source created successfully")
    print(f"Location: {response.headers.get('location')}")
else:
    print(f"Error: {response.text}")
```

##### Option 2: SAS Token Ingestion Source

```python
import requests
import azure.identity
import uuid

# Authenticate
credential = azure.identity.DefaultAzureCredential()
token = credential.get_token("https://geocatalog.spatio.azure.com")
headers = {"Authorization": f"Bearer {token.token}"}

geocatalog_url = "https://your-catalog.westeurope.geocatalog.spatio.azure.com"

# Generate SAS token for your container (using Azure Storage SDK)
from azure.storage.blob import generate_container_sas, ContainerSasPermissions
from datetime import datetime, timedelta

sas_token = generate_container_sas(
    account_name="mpcpstorageaccount",
    container_name="stac-items",
    account_key="your-storage-account-key",
    permission=ContainerSasPermissions(read=True, list=True),
    expiry=datetime.utcnow() + timedelta(days=365)
)

# Define SAS token ingestion source
ingestion_source = {
    "id": str(uuid.uuid4()),
    "kind": "SasToken",
    "connectionInfo": {
        "containerUrl": "https://mpcpstorageaccount.blob.core.windows.net/stac-items",
        "sasToken": sas_token
    }
}

# Create ingestion source
response = requests.post(
    f"{geocatalog_url}/inma/ingestion-sources",
    json=ingestion_source,
    headers=headers,
    params={"api-version": "2025-04-30-preview"}
)

print(f"Status: {response.status_code}")
if response.status_code == 201:
    created_source = response.json()
    print(f"Ingestion source created successfully")
    print(f"ID: {created_source['id']}")
    print(f"Created: {created_source['created']}")
    if created_source['kind'] == 'SasToken':
        print(f"Expiration: {created_source['connectionInfo']['expiration']}")
else:
    print(f"Error: {response.text}")
```

**API Reference:**
- [Ingestion Sources - Create REST API](https://learn.microsoft.com/en-us/rest/api/planetarycomputer/data-plane/ingestion-sources/create?view=rest-planetarycomputer-data-plane-2025-04-30-preview)

### 3. Enable Diagnostic Logging

Enable diagnostic logging to monitor ingestion failures and troubleshoot issues:

```bash
# Create Log Analytics workspace (if you don't have one)
az monitor log-analytics workspace create \
    --resource-group $RESOURCE_GROUP \
    --workspace-name mpcp-logs \
    --location $LOCATION

# Get workspace ID
WORKSPACE_ID=$(az monitor log-analytics workspace show \
    --resource-group $RESOURCE_GROUP \
    --workspace-name mpcp-logs \
    --query id -o tsv)

# Enable diagnostic settings for GeoCatalog
az monitor diagnostic-settings create \
    --name geocatalog-diagnostics \
    --resource /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Orbital/geoCatalogs/$CATALOG_NAME \
    --workspace $WORKSPACE_ID \
    --logs '[{"category": "Ingestion", "enabled": true}]'
```

**Viewing Ingestion Logs:**
```bash
# Query logs for ingestion errors
az monitor log-analytics query \
    --workspace $WORKSPACE_ID \
    --analytics-query "AzureDiagnostics | where Category == 'Ingestion' and Level == 'Error' | order by TimeGenerated desc | take 50"
```

Common error patterns to look for:
- `PublicAccessRestricted`: Missing or expired SAS tokens
- `StacValidationFailed`: Invalid STAC metadata
- `RoleBasedAccessDeny`: Permission issues with managed identity


## Running Processing Pipelines

### COG Creation Pipeline

Process CHIRPS data from UCSB and convert to Cloud Optimized GeoTIFFs.

#### 1. Run Batch Job Creator

```bash
python src/cog_creation/batch_job_creator.py
```

This script:
- Scrapes CHIRPS data URLs from UCSB
- Filters existing items in blob storage
- Creates work item chunks
- Submits Azure Batch job with tasks

#### 2. Monitor Progress

```bash
python src/cog_creation/progress_monitor.py
```

This displays:
- Overall job progress
- Per-task completion status
- Success/failure counts
- ETA for completion

### STAC Creation Pipeline

Convert COG files to STAC items with metadata.

#### 1. Create STAC Catalog

Generate catalog.json from STAC items in blob storage:

```bash
python src/stac_creation/define_catalog.py
```

#### 2. Run Orchestrated Ingestion

Complete workflow: catalog creation → bulk ingestion:

```bash
# Set required environment variables
export STORAGE_ACCOUNT_URL="https://mpcpstorageaccount.blob.core.windows.net"
export CONTAINER_NAME="stac-items"

# Run orchestrator
python src/stac_creation/orchestrate_catalog_ingestion.py \
    --geocatalog-uri "https://geospatialdm.fmd9dgfcd2fab5hw.westeurope.geocatalog.spatio.azure.com" \
    --collection-id "Nigeria-CHIRPS" \
    --collection-title "Nigeria CHIRPS Collection" \
    --collection-desc "CHIRPS v2.0 precipitation data for Nigeria (1981-2025)" \
    --bbox 2.316388 3.837669 15.126447 14.153350 \
    --wait-time 30 \
    --skip-existing-items
```

#### 3. Monitor Ingestion Status

```python
import requests
import azure.identity
import time
import datetime

credential = azure.identity.DefaultAzureCredential()
token = credential.get_token("https://geocatalog.spatio.azure.com")
headers = {"Authorization": f"Bearer {token.token}"}

# Get ingestion location from initial response
location = response.headers["location"]

while True:
    response = requests.get(location, headers=headers)
    status = response.json()["status"]
    print(f"{datetime.datetime.now().isoformat()} - Status: {status}")

    if status not in {"Pending", "Running"}:
        break

    time.sleep(5)
```

---

## Additional Resources

- [Azure Batch Documentation](https://learn.microsoft.com/en-us/azure/batch/)
- [Microsoft Planetary Computer API Documentation](https://learn.microsoft.com/en-us/rest/api/planetary-computer/)
- [Setting Up Ingestion Credentials](https://learn.microsoft.com/en-us/azure/planetary-computer/set-up-ingestion-credentials-managed-identity)

---

**Last Updated:** November 2025

# Troubleshooting Guide

This guide covers common issues and their solutions for the MPCP Lassa Sentinel project.

## Table of Contents

1. [GeoCatalog Issues](#geocatalog-issues)
2. [Azure Batch Issues](#azure-batch-issues)
3. [Storage Issues](#storage-issues)
4. [STAC Validation Issues](#stac-validation-issues)
5. [Performance Issues](#performance-issues)
6. [Permission Issues](#permission-issues)
7. [Visualization Issues](#visualization-issues)

---

## GeoCatalog Issues

### Issue: "LocationNotAvailableForResourceType"

**Error:**
```
The provided location 'uksouth' is not available for resource type 'Microsoft.Orbital/geoCatalogs'
```

**Solution:**
GeoCatalog is only available in: `canadacentral`, `northcentralus`, `westeurope`. Update your `LOCATION` variable:

```bash
export LOCATION="westeurope"
```

### Issue: "HttpRequestPayloadAPISpecValidationFailed" - Catalog Name Pattern

**Error:**
```
String does not match pattern ^[a-zA-Z0-9-]{3,24}$
```

**Solution:**
Catalog names must be 3-24 characters, alphanumeric and hyphens only. No underscores:

```bash
# ❌ Invalid
export CATALOG_NAME="geospatial_disease_modelling"

# ✅ Valid
export CATALOG_NAME="geospatialDM"
```

## Azure Batch Issues

### Issue: "DefaultAzureCredential object has no attribute 'signed_session'"

**Cause:** azure-batch SDK hasn't migrated to Track 2 authentication and doesn't work with azure-identity.

**Solution:**
Use service principal authentication instead of DefaultAzureCredential. Set environment variables:

```bash
export AZURE_TENANT_ID="<your-tenant-id>"
export AZURE_CLIENT_ID="<your-client-id>"
export AZURE_CLIENT_SECRET="<your-client-secret>"
export BATCH_ACCOUNT_URL="<your-batch-url>"
```

### Issue: "environmentSettings is using 294% of available space"

**Cause:** Trying to pass large JSON data (e.g., 550 URLs) as environment variable exceeds size limits (~32KB).

**Solution:**
Use resource files instead of environment variables:

1. Upload work items JSON to blob storage
2. Generate SAS token for the blob
3. Reference as resource file in task definition

This is already implemented in `src/utils/azure_batch_utils.py`.

### Issue: Pool Creation Fails with "End of Support" Error

**Cause:** Using deprecated Ubuntu image version.

**Solution:**
Use Ubuntu 22.04 LTS:

```bash
--image canonical:0001-com-ubuntu-server-jammy:22_04-lts \
--node-agent-sku-id "batch.node.ubuntu 22.04"
```

### Issue: Batch Tasks Fail with "No such file or directory" for Packages

**Symptoms:**
```
stderr.txt:
ERROR: Could not install packages due to an OSError: [Errno 2] No such file or directory:
'/home/conda/feedstock_root/build_artifacts/affine_1733762038348/work'
```

**Cause:** Running `pip freeze` inside a conda environment generates `requirements.txt` with conda build paths instead of package names.

**Solution:**

1. **Create clean requirements.txt:**
```bash
# ❌ Don't do this in conda environment
pip freeze > requirements.txt

# ✅ Instead, manually specify package names
cat > requirements.txt << EOF
requests>=2.31.0
beautifulsoup4>=4.12.0
rasterio>=1.3.0
azure-storage-blob>=12.19.0
azure-batch>=14.0.0
tqdm>=4.66.0
pystac>=1.9.0
planetary-computer>=1.0.0
EOF
```

2. **Or use pip list format:**
```bash
pip list --format=freeze | grep -v "^-e" > requirements.txt
```

3. **Verify requirements.txt contents:**
Ensure all lines follow the format `package==version`, not paths like `/home/conda/...`

### Issue: "InsufficientQuotaAvailable" for Low-Priority Nodes

**Cause:** No low-priority VMs available in the region.

**Solution:**
Switch to spot VMs, by following this [guide](https://learn.microsoft.com/en-us/azure/batch/low-priority-vms-retirement-migration-guide). 

---

## Storage Issues

### Issue: "Public access is not permitted on this storage account"

**Cause:** Blob URLs require authentication but SAS tokens are missing or expired.

**Solution:**

For ingestion from Planetary Computer:
```python
# Use subscription-based signing
import pystac_client
import planetary_computer

catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace
)
```

For your own storage:
```bash
# Regenerate SAS token with appropriate permissions and expiry
az storage container generate-sas \
    --account-name $STORAGE_ACCOUNT_NAME \
    --name <container-name> \
    --permissions r \
    --expiry $(date -u -d "30 days" '+%Y-%m-%dT%H:%MZ') \
    --https-only \
    --output tsv
```

---

## STAC Validation Issues

### Issue: "StacValidationFailed" - Missing 'name' in Classification Classes

**Error:**
```
Classification Extension v2.0.0: each class object must have 'name' field
```

**Solution:**
Fix classification metadata before ingestion:

```python
for item in items:
    asset = item.assets['250m_16_days_pixel_reliability']

    # Add required 'name' fields
    fixed_classes = [
        {"value": 0, "name": "good_data", "description": "Good data"},
        {"value": 1, "name": "marginal_data", "description": "Marginal data"},
        {"value": 2, "name": "snow_ice", "description": "Snow/Ice"},
        {"value": 3, "name": "cloudy", "description": "Cloudy data"}
    ]

    asset.extra_fields['classification:classes'] = fixed_classes
```

This is already handled in `src/ingestion/ingestion_from_datacatalog.py`.

---

## Performance Issues

### Issue: Ingestion Taking Too Long

**Symptoms:**
- Single-item ingestion API too slow for thousands of items
- Connection timeouts after 18+ minutes

**Solution:**

1. Use batch ingestion with smaller batches:
```bash
--batch-size 100  # Instead of 1000+
```

2. Add small delays between batches:
```python
import time
time.sleep(0.5)  # 500ms between batches
```

3. For large datasets, use bulk ingestion API:
```bash
python src/stac_creation/orchestrate_catalog_ingestion.py \
    --geocatalog-uri "..." \
    --collection-id "..." \
    # ... other params
```

### Issue: Search API Rate Limiting

**Symptoms:**
- Slow item searches
- Intermittent failures

**Solution:**
Fetch all items once and filter locally:

```python
# ✅ Efficient: Single search, local filtering
search = catalog.search(collections=[collection], bbox=bbox, datetime=time_range)
items = list(search.get_all_items())
filtered = [item for item in items if condition]

# ❌ Inefficient: Multiple searches
for page in range(num_pages):
    search = catalog.search(...)  # Repeated API calls
```

---

## Permission Issues

### Issue: Service Principal Cannot Access Resources

**Symptoms:**
- 403 Forbidden errors
- "Principal not found" errors

**Solution:**

1. Verify role assignments:

```bash
az role assignment list \
    --assignee $SERVICE_PRINCIPAL_ID \
    --all \
    --output table
```

2. Ensure required roles are assigned:

```bash
# Batch access
az role assignment create \
    --assignee $SERVICE_PRINCIPAL_ID \
    --role "Contributor" \
    --scope /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Batch/batchAccounts/$BATCH_ACCOUNT_NAME

# Storage access
az role assignment create \
    --assignee $SERVICE_PRINCIPAL_ID \
    --role "Storage Blob Data Contributor" \
    --scope /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT_NAME
```

3. Wait for role propagation (can take 5-10 minutes):

```bash
# Test access
az storage blob list \
    --account-name $STORAGE_ACCOUNT_NAME \
    --container-name processed-cogs \
    --auth-mode login \
    --num-results 1
```

---

## Visualization Issues

### Issue: MODIS Data Not Displaying in Explorer

**Symptoms:**
- Collection and items ingested successfully
- Explorer tab visible but map shows no data
- MODIS data in sinusoidal projection

**Cause:** MODIS data uses sinusoidal projection (coordinates in meters) but GeoCatalog Explorer uses Web Mercator projection.

**Solution:**
Add reprojection parameters to render configuration:

```python
render_option = {
    "id": "lst-day",
    "name": "Daytime Land Surface Temp",
    "description": "Daytime LST with automatic reprojection",
    "type": "raster-tile",
    "options": "assets=LST_Day_1km&rescale=10000,20000&colormap_name=viridis&reproject=EPSG:4326&resampling=bilinear",
    "minZoom": 1
}
```

Key parameters:
- `reproject=EPSG:4326`: Reprojects from sinusoidal to lat/lon
- `resampling=bilinear`: Smooths reprojected data

### Issue: Render Config Name Too Long

**Error:**
```
exceeded character limit. Name longer than 40 chars
```

**Solution:**
Limit render option names to 40 characters or less:

```python
# ❌ Too long
render_option["name"] = "Daytime Land Surface Temperature Visualization for West Africa"

# ✅ Valid
render_option["name"] = "Daytime Land Surface Temp"
```

---

## Getting Additional Help

If you encounter an issue not covered in this guide:

1. Check the [SETUP.md](./SETUP.md) for setup instructions
2. Consult Azure documentation:
   - [Azure Batch](https://learn.microsoft.com/en-us/azure/batch/)
   - [Azure Storage](https://learn.microsoft.com/en-us/azure/storage/)
   - [Microsoft Planetary Computer](https://learn.microsoft.com/en-us/azure/planetary-computer/)
3. Check Azure service health: https://status.azure.com/

---

**Last Updated:** November 2025

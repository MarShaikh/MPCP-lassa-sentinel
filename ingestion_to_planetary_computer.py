import json
import random
import time
from datetime import datetime, timedelta
from typing import List

import requests
from azure.identity import AzureCliCredential
from pystac_client import Client
import planetary_computer

# Configuration
geocatalog_url = "https://geospatialdm.fmd9dgfcd2fab5hw.westeurope.geocatalog.spatio.azure.com"
geocatalog_url = geocatalog_url.rstrip("/")
api_version = "2025-04-30-preview"
MPC_APP_ID = "https://geocatalog.spatio.azure.com"

# User selections
# pc_collection = "modis-11A1-061" # for MODIS land surface temperature
pc_collection = "modis-13Q1-061"
bbox_aoi = [2.316388, 3.837669, 15.126447, 14.153350]
param_date_range = "2000-02-18/2025-09-01"

# Token management
_access_token = None
def getBearerToken():
    global _access_token
    if not _access_token or datetime.fromtimestamp(_access_token.expires_on) < datetime.now() + timedelta(minutes=5):
        credential = AzureCliCredential()
        _access_token = credential.get_token(f"{MPC_APP_ID}/.default")
    return {"Authorization": f"Bearer {_access_token.token}"}

def raise_for_status(r: requests.Response) -> None:
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        try:
            print(json.dumps(r.json(), indent=2))
        except:
            print(r.content)
        finally:
            raise

def optimized_batch_ingest(batch_size: int):
    """
    Optimized batch ingestion using ItemCollection endpoint.
    This ingests items directly from Planetary Computer without storing any data.
    
    Args:
        batch_size: Number of items to send per request (max ~500 recommended)
    """
    
    print("Step 1: Fetching collection metadata from Planetary Computer...")
    response = requests.get(
        f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{pc_collection}"
    )
    raise_for_status(response)
    stac_collection = response.json()
    
    # Prepare collection for ingestion
    collection_id = f"{pc_collection}-nigeria-{random.randint(0, 1000)}"
    stac_collection["id"] = collection_id
    stac_collection["title"] = collection_id
    
    # Store original storage info for SAS token
    thumbnail_url = stac_collection.get('assets', {}).get('thumbnail', {}).get('href', '')
    stac_collection.pop('assets', None)
    
    print(f"Step 2: Creating collection in GeoCatalog: {collection_id}")
    collections_endpoint = f"{geocatalog_url}/stac/collections"
    response = requests.post(
        collections_endpoint,
        json=stac_collection,
        headers=getBearerToken(),
        params={"api-version": api_version}
    )
    if response.status_code != 202:
        raise_for_status(response)
    print(f"Collection created: {collection_id}")
    
    if thumbnail_url:
        try:
            print("Adding collection thumbnail...")
            thumbnail_response = requests.get(thumbnail_url)
            if thumbnail_response.status_code == 200:
                collection_assets_endpoint = f"{geocatalog_url}/stac/collections/{collection_id}/assets"
                thumbnail = {"file": ("thumbnail.png", thumbnail_response.content)}
                asset = {
                    "data": '{"key": "thumbnail", "href":"", "type": "image/png", '
                    '"roles": ["thumbnail"], "title": "Collection thumbnail"}'
                }
                response = requests.post(
                    collection_assets_endpoint,
                    data=asset,
                    files=thumbnail,
                    headers=getBearerToken(),
                    params={"api-version": api_version}
                )
        except Exception as e:
            print(f"Could not add thumbnail: {e}")
    
    
    print("Step 3: Searching and ingesting items from Planetary Computer...")
    planetary_computer.set_subscription_key("{{ 53a56e94-45e0-484f-95b7-676b45b31295 }}")
    catalog = Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace
    )
    
    # Collect and process items in batches
    items_endpoint = f"{geocatalog_url}/stac/collections/{collection_id}/items"
    batch = []
    total_ingested = 0
    total_searched = 0
    batch_num = 0
    operation_ids = []
    
    # Create search with explicit limit to avoid timeout
    search = catalog.search(
        collections=[pc_collection], 
        bbox=bbox_aoi, 
        datetime=param_date_range
    )
    
    # items = [item for item in search.item_collection() if item.id.startswith("MOD11A1")]
    items = [item for item in search.item_collection() if item.id.startswith("MOD13Q1")]
    param_max_items = len(items)
    print(f"Searching for items (up to {param_max_items} total)...")
    
    # Process items page by page
    try:
        for item in items:
            item_dict = item.to_dict()
            
            total_searched += 1
            
            # Update collection reference
            item_dict['collection'] = collection_id
            
            # Remove non-static assets
            if 'rendered_preview' in item_dict.get('assets', {}):
                del item_dict['assets']['rendered_preview']
            if 'tilejson' in item_dict.get('assets', {}):
                del item_dict['assets']['tilejson']
            
            batch.append(item_dict)
            
            # Send batch when it reaches the size limit
            if len(batch) >= batch_size:
                batch_num += 1
                print(f"\nIngesting batch {batch_num} ({len(batch)} items)...")
                
                item_collection = {
                    "type": "FeatureCollection",
                    "features": batch
                }
                
                response = requests.post(
                    items_endpoint,
                    json=item_collection,
                    headers=getBearerToken(),
                    params={"api-version": api_version}
                )
                
                if response.status_code in [200, 202]:
                    operation_id = response.json().get('id')
                    operation_ids.append(operation_id)
                    print(f"  Batch {batch_num} accepted. Operation ID: {operation_id}")
                    total_ingested += len(batch)
                else:
                    print(f"  Batch {batch_num} failed: {response.status_code}")
                    print(f"  Error: {response.text}")
                
                # Reset batch
                batch = []
                
                # Small delay between batches to avoid overwhelming the API
                time.sleep(1)
    
    except Exception as e:
        print(f"\nWarning: Search interrupted - {e}")
        print(f"Continuing with {len(batch)} items in current batch...")
    
    # Send any remaining items
    if batch:
        batch_num += 1
        print(f"\nIngesting final batch {batch_num} ({len(batch)} items)...")
        
        item_collection = {
            "type": "FeatureCollection",
            "features": batch
        }
        
        response = requests.post(
            items_endpoint,
            json=item_collection,
            headers=getBearerToken(),
            params={"api-version": api_version}
        )
        
        if response.status_code in [200, 202]:
            operation_id = response.json().get('id')
            operation_ids.append(operation_id)
            print(f"  Final batch accepted. Operation ID: {operation_id}")
            total_ingested += len(batch)
        else:
            print(f"  Final batch failed: {response.status_code}")
    
    print(f"\n✅ Ingestion complete!")
    print(f"   Total items submitted: {total_ingested}")
    print(f"   Total batches: {batch_num}")
    print(f"   Collection ID: {collection_id}")
    
    return collection_id, operation_ids

def monitor_ingestion_status(status_url: str, timeout_seconds: int = 3600):
    """Monitor the ingestion status."""
    start_time = time.time()
    
    while time.time() - start_time < timeout_seconds:
        response = requests.get(status_url, headers=getBearerToken())
        if response.status_code == 200:
            status = response.json()
            print(f"Ingestion status: {status.get('status', 'Unknown')}")
            
            if status.get("status") in ["Succeeded", "Completed"]:
                print("Ingestion completed successfully!")
                break
            elif status.get("status") in ["Failed", "Canceled"]:
                print(f"Ingestion failed: {status}")
                break
        
        time.sleep(30)  # Check every 30 seconds

# ========== MONITORING UTILITIES ==========

def monitor_ingestion_operations(operation_ids: List[str], timeout_seconds: int = 1800):
    """
    Monitor multiple ingestion operations.
    
    Args:
        operation_ids: List of operation IDs to monitor
        timeout_seconds: Maximum time to wait for all operations
    """
    start_time = time.time()
    completed = []
    failed = []
    
    while time.time() - start_time < timeout_seconds:
        remaining = [op_id for op_id in operation_ids if op_id not in completed and op_id not in failed]
        
        if not remaining:
            print("\n✅ All operations completed!")
            print(f"   Successful: {len(completed)}")
            print(f"   Failed: {len(failed)}")
            break
        
        print(f"\nChecking status... ({len(remaining)} operations remaining)")
        
        for op_id in remaining[:5]:  # Check up to 5 at a time
            # You'll need to determine the correct status endpoint format
            status_url = f"{geocatalog_url}/inma/operations/{op_id}"
            
            try:
                response = requests.get(status_url, headers=getBearerToken(), params={"api-version": api_version})
                if response.status_code == 200:
                    status = response.json().get('status', 'Unknown')
                    
                    if status in ["Succeeded", "Completed"]:
                        completed.append(op_id)
                        print(f"  ✓ {op_id}: Completed")
                    elif status in ["Failed", "Canceled"]:
                        failed.append(op_id)
                        print(f"  ✗ {op_id}: Failed")
            except Exception as e:
                print(f"  ? {op_id}: Error checking status - {e}")
        
        time.sleep(30)  # Check every 30 seconds
    
    if time.time() - start_time >= timeout_seconds:
        print(f"\n⚠️ Timeout reached after {timeout_seconds} seconds")

def verify_ingestion(collection_id: str) -> int:
    """
    Verify how many items were successfully ingested.
    
    Returns:
        Number of items in the collection
    """
    stac_search_endpoint = f"{geocatalog_url}/stac/search"
    
    response = requests.get(
        stac_search_endpoint,
        json={"collection": [collection_id]},
        headers=getBearerToken(),
        params={"api-version": api_version, "sign": "true"}
    )
    
    
    if response.status_code == 200:
        result = response.json()['features']
        total = len(result)
        print(f"\n📊 Collection {collection_id} now contains {total} items")
        return total
    else:
        print(f"\n⚠️ Could not verify collection items: {response.status_code}")
        return 0

# ========== MAIN EXECUTION ==========

if __name__ == "__main__":
    print("=" * 60)
    print("OPTIMIZED PLANETARY COMPUTER PRO INGESTION")
    print("=" * 60)
    print(f"Collection: {pc_collection}")
    print(f"Bounding box: {bbox_aoi}")
    print(f"Date range: {param_date_range}")
    print("=" * 60)
    
    try:
        collection_id, operation_ids = optimized_batch_ingest(batch_size=100)
        
        if operation_ids:
            print("\nMonitoring ingestion operations...")
            monitor_ingestion_operations(operation_ids)

        # Verify the ingestion
        time.sleep(10)  # Give it a moment to process
        verify_ingestion(collection_id)
        
    except Exception as e:
        print(f"\n❌ Error during ingestion: {e}")
        import traceback
        traceback.print_exc()
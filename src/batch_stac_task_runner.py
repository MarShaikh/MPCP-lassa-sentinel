"""
Batch task runner for converting COG files to STAC items on Azure Batch nodes.
"""

import os
import json
import sys
import traceback
from datetime import datetime
from typing import List, Dict

from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.identity import DefaultAzureCredential

# Import the STAC conversion functions
from stac_conversion import process_cog_to_stac, save_stac_item_to_blob


def get_work_items_from_file() -> List[Dict]:
    """
    Reads work items from a JSON file in the task's working directory.
    The file is downloaded by the Batch service via a ResourceFile.
    
    Returns:
        List[Dict]: List of work items to process
    """
    # AZ_BATCH_TASK_WORKING_DIR is a default environment variable set by Batch.
    task_working_dir = os.environ.get("AZ_BATCH_TASK_WORKING_DIR")
    file_path = os.path.join(task_working_dir, "work_items.json")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Work items file not found at: {file_path}")
    
    try:
        with open(file_path, 'r') as f:
            work_items = json.load(f)
        return work_items
    except (json.JSONDecodeError, IOError) as e:
        raise ValueError(f"Failed to read or parse work_items.json: {e}")


def setup_working_directories():
    """
    Creates necessary directories for processing on the VM.
    """
    base_dir = "/tmp/stac_processing"
    os.makedirs(os.path.join(base_dir, "stac-items"), exist_ok=True)
    os.makedirs("/tmp/batch-logs", exist_ok=True)
    return base_dir


def process_single_cog(work_item: Dict, cog_container_client: ContainerClient,
                       stac_blob_service_client: BlobServiceClient) -> Dict:
    """
    Process a single COG file to create and store a STAC item.
    
    Parameters:
        work_item (Dict): Work item with COG file information
        cog_container_client: Container client for COG files
        stac_blob_service_client: Blob service client for storing STAC items
        
    Returns:
        Dict: Result dictionary with success status and details
    """
    try:
        year = work_item['year']
        filename = work_item['filename']
        blob_path = work_item['blob_path']
        
        # Get blob client for the COG file
        blob_client = cog_container_client.get_blob_client(blob_path)
        
        # Process COG to STAC
        blob_domain = os.environ["STORAGE_ACCOUNT_URL"]
        stac_item_dict = process_cog_to_stac(
            blob_client=blob_client,
            filename=filename,
            blob_domain=blob_domain,
            container_name="processed-cogs",
            year=year
        )
        
        # Save STAC item to blob storage
        stac_filename = filename.replace('.tif', '.json')
        stac_blob_path = f"{year}/{stac_filename}"
        
        save_stac_item_to_blob(
            stac_item_dict=stac_item_dict,
            blob_service_client=stac_blob_service_client,
            container_name="stac-items",
            blob_path=stac_blob_path
        )
        
        return {
            "success": True,
            "blob_path": blob_path,
            "stac_path": stac_blob_path
        }
        
    except Exception as e:
        return {
            "success": False,
            "blob_path": blob_path,
            "error": str(e),
            "traceback": traceback.format_exc()
        }


def process_batch_with_progress(work_items: List[Dict]):
    """
    Process a batch of COG files to create STAC items with progress tracking.
    
    Parameters:
        work_items (List[Dict]): List of work items to process
    """
    task_id = os.environ.get('AZ_BATCH_TASK_ID', f'local_task_{int(datetime.now().timestamp())}')
    
    # Setup Azure connections
    storage_account_url = os.environ["STORAGE_ACCOUNT_URL"]
    
    # Create blob service clients with SAS tokens
    cog_sas = os.environ["COG_CONTAINER_SAS"]
    stac_sas = os.environ["STAC_CONTAINER_SAS"]
    logs_sas = os.environ["LOGS_CONTAINER_SAS"]
    
    # COG container client (read-only)
    cog_blob_service_client = BlobServiceClient(
        account_url=f"{storage_account_url}?{cog_sas}"
    )
    cog_container_client = cog_blob_service_client.get_container_client("processed-cogs")
    
    # STAC container client (read-write)
    stac_blob_service_client = BlobServiceClient(
        account_url=f"{storage_account_url}?{stac_sas}"
    )
    
    # Progress tracking
    completed = []
    failed = []
    
    print(f"Task {task_id}: Starting to process {len(work_items)} COG files")
    
    for i, work_item in enumerate(work_items):
        print(f"Processing {i+1}/{len(work_items)}: {work_item['blob_path']}")
        
        result = process_single_cog(
            work_item=work_item,
            cog_container_client=cog_container_client,
            stac_blob_service_client=stac_blob_service_client
        )
        
        if result["success"]:
            completed.append(result)
            print(f"  ✅ Successfully created STAC item: {result['stac_path']}")
        else:
            failed.append(result)
            print(f"  ❌ Failed: {result['error']}")
        
        # Update progress every 10 items or at the end
        if (i + 1) % 10 == 0 or i == len(work_items) - 1:
            update_progress_file(
                task_id=task_id,
                completed=completed,
                failed=failed,
                total=len(work_items),
                storage_account_url=storage_account_url,
                logs_sas=logs_sas
            )
    
    print(f"Task {task_id}: Completed {len(completed)}/{len(work_items)} items successfully")
    if failed:
        print(f"Failed items: {len(failed)}")
        for fail in failed[:5]:  # Show first 5 failures
            print(f"  - {fail['blob_path']}: {fail['error']}")


def update_progress_file(task_id: str, completed: List[Dict], failed: List[Dict],
                         total: int, storage_account_url: str, logs_sas: str):
    """
    Update progress file and upload to Azure Blob Storage.
    
    Parameters:
        task_id (str): Task identifier
        completed (List[Dict]): List of completed items
        failed (List[Dict]): List of failed items
        total (int): Total number of items to process
        storage_account_url (str): Storage account URL
        logs_sas (str): SAS token for logs container
    """
    progress_data = {
        "task_id": task_id,
        "timestamp": datetime.now().isoformat(),
        "total_items": total,
        "completed_count": len(completed),
        "failed_count": len(failed),
        "progress_percentage": (len(completed) / total * 100) if total > 0 else 0,
        "completed_items": [c["stac_path"] for c in completed[-10:]],  # Last 10
        "failed_items": [{"path": f["blob_path"], "error": f["error"]} for f in failed]
    }
    
    # Save locally first
    temp_dir = "/tmp/batch-logs"
    os.makedirs(temp_dir, exist_ok=True)
    file_name = f"stac_{task_id}_progress.json"
    local_file_path = os.path.join(temp_dir, file_name)
    
    with open(local_file_path, 'w') as f:
        json.dump(progress_data, f, indent=2)
    
    # Upload to blob storage
    try:
        blob_service_client = BlobServiceClient(
            account_url=f"{storage_account_url}?{logs_sas}"
        )
        blob_client = blob_service_client.get_blob_client(
            container="batch-logs",
            blob=file_name
        )
        
        with open(local_file_path, 'rb') as f:
            blob_client.upload_blob(f, overwrite=True)
        
        print(f"Progress updated: {len(completed)}/{total} completed")
        
    except Exception as e:
        print(f"Failed to upload progress file: {e}")


def main():
    """
    Main function to run the batch task for STAC processing.
    """
    try:
        print("Starting STAC batch task runner...")
        task_id = os.environ.get('AZ_BATCH_TASK_ID', 'unknown_task')
        print(f"Task ID: {task_id}")
        
        # Setup working directories
        setup_working_directories()
        
        # Get work items from file
        work_items = get_work_items_from_file()
        print(f"Loaded {len(work_items)} work items from resource file")
        
        # Process the batch
        process_batch_with_progress(work_items)
        
        print(f"Task {task_id} completed successfully")
        
    except Exception as e:
        print(f"Task failed with error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
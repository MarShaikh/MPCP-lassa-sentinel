"""
Batch job creator for converting Geo-TIFF COG files to STAC item in Azure Batch
"""
import json 
import traceback
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import os

from azure.batch.custom.custom_errors import CreateTasksErrorException
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

from src.utils.batch_task_utils import create_chunks
from src.utils.azure_batch_utils import (
    create_batch_job_with_pool,
    generate_sas_tokens_for_containers,
    create_task_data_blob_client,
    upload_work_items_and_create_task
)


def create_batch_job():
    """
    Create a batch job for STAC processing.

    Returns:
        tuple: BatchServiceClient and job_id
    """
    return create_batch_job_with_pool("stac-processing")


def get_cog_files_to_process() -> List[Dict]:
    """
    Get list of COG files from processed-cogs container that need STAC items

    Returns: 
        List[Dict]: List of work items with COG file information
    """

    STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
    storage_credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(
        account_url=STORAGE_ACCOUNT_URL,
        credential=storage_credential
    )

    # Get all COG files from processed-cogs container
    container_client = blob_service_client.get_container_client("processed-cogs")
    
    work_items = []
    for blob in container_client.list_blobs():
        if blob.name.endswith('.tif'):
            # Extract year from blob path (format: year/filename.tif)
            parts = blob.name.split('/')
            if len(parts) == 2:
                year = parts[0]
                filename = parts[1]
                work_items.append({
                    "year": year,
                    "filename": filename,
                    "blob_path": blob.name
                })
    
    print(f"Found {len(work_items)} COG files to process")
    return work_items


def filter_existing_stac_items(work_items: List[Dict]) -> List[Dict]:
    """
    Filter out work items whose STAC items already exist in stac-items container.
    
    Parameters:
        work_items (List[Dict]): List of work items with COG file information
        
    Returns:
        List[Dict]: Filtered list of work items that need processing
    """
    try:
        STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
        storage_credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=STORAGE_ACCOUNT_URL,
            credential=storage_credential
        )
        
        # Check if stac-items container exists, create if not
        container_client = blob_service_client.get_container_client("stac-items")
        try:
            container_client.get_container_properties()
        except:
            container_client.create_container()
            print("Created stac-items container")
        
        # Get list of existing STAC items
        existing_stac_items = {blob.name for blob in container_client.list_blobs()}
        print(f"Found {len(existing_stac_items)} existing STAC items")
        
        # Filter work items
        filtered_work_items = []
        for work_item in work_items:
            # Predict the STAC item blob path
            stac_filename = work_item['filename'].replace('.tif', '.json')
            stac_blob_path = f"{work_item['year']}/{stac_filename}"
            
            if stac_blob_path not in existing_stac_items:
                filtered_work_items.append(work_item)
        
        filtered_count = len(work_items) - len(filtered_work_items)
        print(f"Filtered out {filtered_count} work items that already have STAC items")
        print(f"{len(filtered_work_items)} work items remain to process")
        
        return filtered_work_items
        
    except Exception as e:
        print(f"Failed to filter work items: {e}")
        traceback.print_exc()
        raise


def create_and_submit_tasks(batch_client, job_id: str, work_items_chunks: List[List[Dict]]):
    """
    Upload task data to Azure Storage and submit tasks with ResourceFiles.

    Parameters:
        batch_client: Azure Batch client
        job_id (str): Batch job ID
        work_items_chunks (List[List[Dict]]): Chunks of work items to process
    """
    # Setup Azure Storage and create task-data container if needed
    blob_service_client, container_name = create_task_data_blob_client(ensure_container=True)

    # Generate SAS tokens for containers
    container_configs = {
        "processed-cogs": {"read": True, "list": True},
        "stac-items": {"read": True, "write": True, "create": True, "list": True},
        "batch-logs": {"read": True, "write": True, "create": True, "list": True}
    }
    sas_tokens = generate_sas_tokens_for_containers(container_configs)

    # Build environment variables
    STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
    env_vars = {
        "STORAGE_ACCOUNT_URL": STORAGE_ACCOUNT_URL,
        "COG_CONTAINER_SAS": sas_tokens["processed-cogs"],
        "STAC_CONTAINER_SAS": sas_tokens["stac-items"],
        "LOGS_CONTAINER_SAS": sas_tokens["batch-logs"]
    }

    tasks = []
    for i, chunk in enumerate(work_items_chunks):
        task_id = f"stac_task{i:03d}"

        task = upload_work_items_and_create_task(
            blob_service_client=blob_service_client,
            container_name=container_name,
            job_id=job_id,
            task_id=task_id,
            work_items_chunk=chunk,
            env_vars=env_vars,
            script_path="src/batch_stac_task_runner.py"
        )
        tasks.append(task)

    # Submit all tasks at once
    print(f"Submitting {len(tasks)} tasks to job {job_id}")
    batch_client.task.add_collection(job_id, tasks)
    print("All tasks submitted successfully!")


def main():
    """
    Main function to orchestrate the batch job creation for STAC processing.
    """
    try:
        # Get list of COG files to process
        work_items = get_cog_files_to_process()
        
        if not work_items:
            print("No COG files found to process. Exiting.")
            return
        
        # Filter out already processed items
        filtered_work_items = filter_existing_stac_items(work_items)
        
        if not filtered_work_items:
            print("All COG files already have STAC items! No new job needed.")
            return
        
        # Create chunks for batch processing
        work_items_chunks = create_chunks(filtered_work_items, chunk_size=100)

        print(f"Created {len(work_items_chunks)} chunks to be submitted as tasks.")
        
        # Create batch job and submit tasks
        batch_client, job_id = create_batch_job()
        create_and_submit_tasks(batch_client, job_id, work_items_chunks)
        
        print(f"Job '{job_id}' created with {len(work_items_chunks)} tasks.")
        print(f"Processing {len(filtered_work_items)} COG files total.")
        
    except CreateTasksErrorException as e:
        print("An error occurred while adding tasks.")
        print("Printing details for each failed task...")
        for failure in e.failure_tasks:
            print(f"  - Task ID: {failure.task_id}")
            print(f"    - Error Code: {failure.error.code}")
            print(f"    - Error Message: {failure.error.message}")
            
            if failure.error.values:
                for detail in failure.error.values:
                    print(f"      - Detail Key: {detail.key}, Value: {detail.value}")
        
        traceback.print_exc()
        
    except Exception as e:
        print(f"An error occurred during job or task creation: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()

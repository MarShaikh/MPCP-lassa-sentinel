"""
Batch job creator for converting Geo-TIFF COG files to STAC item in Azure Batch
"""
import json 
import traceback
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import os

from azure.batch import BatchServiceClient
from azure.batch.models import JobAddParameter, PoolInformation, TaskAddParameter, ResourceFile
from azure.common.credentials import ServicePrincipalCredentials
from azure.batch.custom.custom_errors import CreateTasksErrorException
from azure.storage.blob import (
    BlobServiceClient, generate_blob_sas, generate_container_sas, 
    BlobSasPermissions, ContainerSasPermissions
)
from azure.identity import DefaultAzureCredential


def create_batch_job():
    """
    Create a batch job for STAC processing.
    
    Returns:
        tuple: BatchServiceClient and job_id
    """
    TENANT_ID = os.environ["AZURE_TENANT_ID"]
    CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
    CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
    BATCH_ACCOUNT_URL = os.environ["BATCH_ACCOUNT_URL"]
    RESOURCE = "https://batch.core.windows.net/"

    credentials = ServicePrincipalCredentials(
        client_id=CLIENT_ID,
        secret=CLIENT_SECRET,
        tenant=TENANT_ID,
        resource=RESOURCE
    )

    batch_client = BatchServiceClient(
        credentials,
        batch_url=BATCH_ACCOUNT_URL
    )

    # Create a unique job ID with timestamp
    job_id = f"stac-processing-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    
    # Create job configuration
    job = JobAddParameter(
        id=job_id,
        pool_info=PoolInformation(pool_id="geospatial-processing-pool")
    )

    # Create the job
    print(f"Creating job: {job_id}")
    batch_client.job.add(job)
    
    return batch_client, job_id


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


def create_chunks(work_items: List[Dict], chunk_size: int = 100) -> List[List[Dict]]:
    """
    Split work items into chunks for batch processing.
    
    Parameters:
        work_items (List[Dict]): List of work items
        chunk_size (int): Number of items per chunk
        
    Returns:
        List[List[Dict]]: List of chunks
    """
    chunks = []
    for i in range(0, len(work_items), chunk_size):
        chunk = work_items[i:i + chunk_size]
        chunks.append(chunk)
    return chunks


def create_and_submit_tasks(batch_client, job_id: str, work_items_chunks: List[List[Dict]]):
    """
    Upload task data to Azure Storage and submit tasks with ResourceFiles.
    
    Parameters:
        batch_client: Azure Batch client
        job_id (str): Batch job ID
        work_items_chunks (List[List[Dict]]): Chunks of work items to process
    """
    tasks = []
    
    # Setup Azure Storage
    STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
    STORAGE_ACCOUNT_NAME = STORAGE_ACCOUNT_URL.split("//")[1].split(".")[0]
    CONTAINER_NAME = "task-data"
    BATCH_STORAGE_ACCOUNT_KEY = os.environ["BATCH_STORAGE_ACCOUNT_KEY"]
    
    storage_credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(
        account_url=STORAGE_ACCOUNT_URL,
        credential=storage_credential
    )
    
    # Check/create task-data container
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    try:
        container_client.get_container_properties()
    except:
        container_client.create_container()
        print(f"Created {CONTAINER_NAME} container")
    
    # Generate SAS tokens for containers (valid for 7 days)
    cog_sas = generate_container_sas(
        account_name=STORAGE_ACCOUNT_NAME,
        container_name="processed-cogs",
        account_key=BATCH_STORAGE_ACCOUNT_KEY,
        permission=ContainerSasPermissions(read=True, list=True),
        expiry=datetime.now(timezone.utc) + timedelta(days=7)
    )
    
    stac_sas = generate_container_sas(
        account_name=STORAGE_ACCOUNT_NAME,
        container_name="stac-items",
        account_key=BATCH_STORAGE_ACCOUNT_KEY,
        permission=ContainerSasPermissions(read=True, write=True, create=True, list=True),
        expiry=datetime.now(timezone.utc) + timedelta(days=7)
    )
    
    logs_sas = generate_container_sas(
        account_name=STORAGE_ACCOUNT_NAME,
        container_name="batch-logs",
        account_key=BATCH_STORAGE_ACCOUNT_KEY,
        permission=ContainerSasPermissions(read=True, write=True, create=True, list=True),
        expiry=datetime.now(timezone.utc) + timedelta(days=7)
    )
    
    for i, chunk in enumerate(work_items_chunks):
        task_id = f"stac_task{i:03d}"
        
        # Upload work items to blob
        work_items_json = json.dumps(chunk)
        blob_name = f"{job_id}/{task_id}_work_items.json"
        
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=blob_name
        )
        blob_client.upload_blob(work_items_json.encode('utf-8'), overwrite=True)
        print(f"Uploaded data for {task_id} to blob: {blob_name}")
        
        # Create SAS URL for batch nodes
        sas_token = generate_blob_sas(
            account_name=STORAGE_ACCOUNT_NAME,
            container_name=CONTAINER_NAME,
            blob_name=blob_name,
            account_key=BATCH_STORAGE_ACCOUNT_KEY,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(days=7)
        )
        blob_sas_url = f"{blob_client.url}?{sas_token}"
        
        # Create a resource file
        resource_file = ResourceFile(
            http_url=blob_sas_url,
            file_path="work_items.json"
        )
        
        # Command line to run on batch node
        command_line = (
            "/bin/bash -c '"
            "export STORAGE_ACCOUNT_URL=\"" + STORAGE_ACCOUNT_URL + "\" && "
            "export COG_CONTAINER_SAS=\"" + cog_sas + "\" && "
            "export STAC_CONTAINER_SAS=\"" + stac_sas + "\" && "
            "export LOGS_CONTAINER_SAS=\"" + logs_sas + "\" && "
            "cd /tmp && "
            "[ -d code ] && rm -rf code; "
            "git clone https://github.com/MarShaikh/MPCP-lassa-sentinel.git code && "
            "cd code && "
            "python3.11 -m pip install -r requirements.txt && "
            "python3.11 src/batch_stac_task_runner.py'"
        )
        
        task = TaskAddParameter(
            id=task_id,
            command_line=command_line,
            resource_files=[resource_file]
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

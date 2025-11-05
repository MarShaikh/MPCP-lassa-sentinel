import json
import traceback
from datetime import datetime, timedelta, timezone
from typing import List
import os


from src.batch_processing.data_extraction import find_tiff_url
from src.utils.batch_task_utils import create_chunks
from src.utils.azure_batch_utils import (
    create_batch_job_with_pool,
    generate_sas_tokens_for_containers,
    create_task_data_blob_client,
    upload_work_items_and_create_task
)

from azure.batch.custom.custom_errors import CreateTasksErrorException
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

def create_batch_job():
    """
    Create a batch job for CHIRPS processing.

    Returns:
        tuple: BatchServiceClient and job_id
    """
    return create_batch_job_with_pool("chirps-processing")


def create_and_submit_tasks(batch_client, job_id, work_items_chunks):
    """
    Uploads task data to Azure Storage and submits tasks with ResourceFiles
    """
    # Setup Azure Storage (no need to ensure container for CHIRPS)
    blob_service_client, container_name = create_task_data_blob_client(ensure_container=False)

    # Generate SAS tokens for containers
    container_configs = {
        "processed-cogs": {"read": True, "write": True, "create": True, "list": True},
        "raw-data": {"read": True, "write": True, "create": True, "list": True},
        "batch-logs": {"read": True, "write": True, "create": True, "list": True}
    }
    sas_tokens = generate_sas_tokens_for_containers(container_configs)

    # Build environment variables
    STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
    env_vars = {
        "STORAGE_ACCOUNT_URL": STORAGE_ACCOUNT_URL,
        "COG_CONTAINER_SAS": sas_tokens["processed-cogs"],
        "RAW_CONTAINER_SAS": sas_tokens["raw-data"],
        "LOGS_CONTAINER_SAS": sas_tokens["batch-logs"]
    }

    tasks = []
    for i, chunk in enumerate(work_items_chunks):
        task_id = f"task{i:03d}"

        task = upload_work_items_and_create_task(
            blob_service_client=blob_service_client,
            container_name=container_name,
            job_id=job_id,
            task_id=task_id,
            work_items_chunk=chunk,
            env_vars=env_vars,
            script_path="src/batch_processing/batch_task_runner.py"
        )
        tasks.append(task)

    # Submit all tasks at once
    print(f"Submitting {len(tasks)} tasks to job {job_id}")
    batch_client.task.add_collection(job_id, tasks)
    print("All tasks submitted successfully!")

def filter_existing_work_items(work_items: List[dict]) -> List[dict]:
    """
    Filter out work items whose COG files already exist in processed-cogs container.
    
    Parameters
    ----------
    work_items : List[dict]
        List of work items with 'year' and 'url' keys
    
    Returns
    -------
    List[dict]
        Filtered list of work items that need processing (COGs don't exist yet)
    """
    try:
        # Setup Azure Storage connection
        STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
        storage_credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=STORAGE_ACCOUNT_URL, credential=storage_credential)
        
        # Get container client and list all existing COGs
        container_client = blob_service_client.get_container_client("processed-cogs")
        existing_cogs = {blob.name for blob in container_client.list_blobs()}
        
        print(f"Found {len(existing_cogs)} existing COG files in processed-cogs container")
        
        # Filter work items
        filtered_work_items = []
        for work_item in work_items:
            year = work_item['year']
            url = work_item['url']
            year_dir = str(year) + "/"
            
            # Predict the blob path
            raw_file_name = url.split(year_dir)[1].replace(".gz", "")
            cog_file_name = f"nigeria-cog-{raw_file_name}"
            blob_path = f"{year}/{cog_file_name}"
            
            # Keep only if it doesn't exist
            if blob_path not in existing_cogs:
                filtered_work_items.append(work_item)
        
        filtered_out_count = len(work_items) - len(filtered_work_items)
        print(f"Filtered out {filtered_out_count} work items that already exist")
        print(f"{len(filtered_work_items)} work items remain to process")
        
        return filtered_work_items
        
    except Exception as e:
        print(f"Failed to filter work items: {e}")
        traceback.print_exc()
        raise

def main():
    # url to the file system
    url = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05/"
    year_urls = find_tiff_url(url, pattern = r"\d{4}\/")
    
    # ===============================Note============================================
    # get links to all TIFF files
    # data_urls is a list of a list with 45 years worth of data from 1981-2025
    # where index 0 has all data for 1981 and index 1 has 1982 ... index 44 has 2025
    # converting to a list of dicts to make it easy to work with downstream
    # ===============================================================================
    print(f"Found {len(year_urls)} year URLs. First one is: {year_urls[0] if year_urls else 'None'}")
    if not year_urls:
        print("Stopping script because no year URLs were found. Check data_extraction.py or the website structure.")
        return
    
    data_urls = []
    for i, year in enumerate(year_urls):
        # urls per year
        urls = find_tiff_url(year, pattern = r"chirps-.*")
        data_urls.append({"year": str(i + 1981), "urls": urls})
    
    # iterate through all the years, and convert to COGS
    work_items = []
    for data in data_urls:
        for url in data['urls']:
            work_items.append({"year": data['year'], "url": url})
    
    print(f"Created a total of {len(work_items)} work items.")
    if not work_items:
        print("Stopping script because no work items were generated from the URLs.")
        return
    
    # Filter out existing COGs
    filtered_work_items = filter_existing_work_items(work_items)
    
    if not filtered_work_items:
        print("All work items already processed! No new job needed.")
        return
    
    work_items_chunks = create_chunks(filtered_work_items, chunk_size=550)
    
    print(f"Created {len(work_items_chunks)} chunks to be submitted as tasks.")
    if not work_items_chunks:
        print("Stopping script because no chunks were created.")
        return

    try:
        batch_client, job_id = create_batch_job()
        create_and_submit_tasks(batch_client, job_id, work_items_chunks)
        print(f"Job '{job_id}' created with {len(work_items_chunks)} tasks.")
    
    except CreateTasksErrorException as e:
        print("An error occurred while adding tasks.")
        print("Printing details for each failed task...")
        for failure in e.failure_tasks:
            print(f"  - Task ID: {failure.task_id}")
            print(f"    - Error Code: {failure.error.code}")
            print(f"    - Error Message: {failure.error.message}")

            if failure.error.values:
                if failure.error.values:
                    for detail in failure.error.values:
                        print(f"      - Detail Key: {detail.key}, Value: {detail.value}")
        
        traceback.print_exc()

    except Exception as e:
        print(f"An error occurred during job or task creation: {e}")
        # Print the full traceback to get more details on the error
        traceback.print_exc()


if __name__ == "__main__":
    main()
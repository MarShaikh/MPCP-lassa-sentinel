import json
import traceback
from datetime import datetime, timedelta, timezone
import os


from data_extraction import find_tiff_url
from processing import create_chunks

from azure.batch import  BatchServiceClient
from azure.batch.models import JobAddParameter, PoolInformation, TaskAddParameter, ResourceFile
from azure.common.credentials import ServicePrincipalCredentials
from azure.batch.custom.custom_errors import CreateTasksErrorException
from azure.storage.blob import BlobServiceClient, generate_blob_sas, generate_container_sas, BlobSasPermissions, ContainerSasPermissions
from azure.identity import DefaultAzureCredential

def create_batch_job():
    
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

    # create a unique job ID with timestamp
    job_id = f"chirps-processing-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    # create job configuration
    job = JobAddParameter(
        id = job_id, 
        pool_info = PoolInformation(pool_id="geospatial-processing-pool")
    )

    # create the job
    print(f"Creating job: {job_id}")
    batch_client.job.add(job)

    return batch_client, job_id


def create_and_submit_tasks(batch_client, job_id, work_items_chunks):
    """
    Uploads task data to Azure Storage and submits tasks with ResourceFiles
    """
    tasks = []

    # setup Azure Storage
    STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
    STORAGE_ACCOUNT_NAME = STORAGE_ACCOUNT_URL.split("//")[1].split(".")[0]
    CONTAINER_NAME = "task-data"
    BATCH_STORAGE_ACCOUNT_KEY = os.environ["BATCH_STORAGE_ACCOUNT_KEY"]
    
    storage_credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=STORAGE_ACCOUNT_URL, credential=storage_credential)

    # Generate SAS tokens for output containers (valid for 7 days)
    cog_sas = generate_container_sas(
        account_name=STORAGE_ACCOUNT_NAME,
        container_name="processed-cogs",
        account_key=BATCH_STORAGE_ACCOUNT_KEY,
        permission=ContainerSasPermissions(read=True, write=True, create=True, list=True),
        expiry=datetime.now(timezone.utc) + timedelta(days=7)
    )

    raw_sas = generate_container_sas(
        account_name=STORAGE_ACCOUNT_NAME,
        container_name="raw-data",
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
        task_id = f"task{i:03d}"

        # upload work items to blob
        work_items_json = json.dumps(chunk)
        blob_name = f"{job_id}/{task_id}_work_items.json"
        
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        blob_client.upload_blob(work_items_json.encode('utf-8'), overwrite=True)
        print(f"Uploaded data for {task_id} to blob: {blob_name}")

        # creating SAS URL for batch nodes to get temp access to the file
        sas_token = generate_blob_sas(
            account_name=STORAGE_ACCOUNT_NAME,
            container_name=CONTAINER_NAME,
            blob_name=blob_name,
            account_key=BATCH_STORAGE_ACCOUNT_KEY,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(days=7)
        )
        blob_sas_url = f"{blob_client.url}?{sas_token}"

        # create a resource file; this tells batch to download the file from the SAS url before running the command
        # file path is the name it will have on the compute node
        resource_file = ResourceFile(
            http_url=blob_sas_url,
            file_path=f"work_items.json"
        )

        command_line = (
            "/bin/bash -c '"
            "export STORAGE_ACCOUNT_URL=\"" + STORAGE_ACCOUNT_URL + "\" && "
            "export COG_CONTAINER_SAS=\"" + cog_sas + "\" && "
            "export RAW_CONTAINER_SAS=\"" + raw_sas + "\" && "
            "export LOGS_CONTAINER_SAS=\"" + logs_sas + "\" && "
            "cd /tmp && "
            "[ -d code ] && rm -rf code; "
            "git clone https://github.com/MarShaikh/MPCP-lassa-sentinel.git code && "
            "cd code && "
            "python3.11 -m pip install -r requirements.txt && "
            "python3.11 src/batch_task_runner.py'"
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
    
    
    work_items_chunks = create_chunks(work_items)
    
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
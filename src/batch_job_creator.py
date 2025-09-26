import json
from datetime import datetime
from typing import List

from data_extraction import find_tiff_url
from processing import create_chunks

from azure.batch import  BatchServiceClient
from azure.identity import DefaultAzureCredential
from azure.batch.models import JobAddParameter, PoolInformation, TaskAddParameter, EnvironmentSetting


def create_batch_job():
    credential = DefaultAzureCredential()
    batch_client = BatchServiceClient(
        credentials=credential,
        batch_url="https://mpcp-batch-account.uksouth.batch.azure.com"
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
    tasks = []

    for i, chunk in enumerate(work_items_chunks):
        task_id = f"task_{i:03d}"  # task_000, task_001, etc.

        # convert work items to JSON for environment variable
        work_items_json = json.dumps(chunk)

        # command to run on each VM
        command_line = (
            "/bin/bash -c '" 
            "cd /tmp && "
            "git clone <github repo> code && "
            "cd code && "
            "pip install -r requirements.txt && "
            "python batch_task_runner.py '"
        )

        # create task with environment variable
        task = TaskAddParameter(
            id = task_id, 
            command_line = command_line,
            environment_settings=[
                EnvironmentSetting(name="WORK_ITEMS_JSON", value=work_items_json)
            ]
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
    
    
    work_items_chunks = create_chunks(work_items)

    try:
        batch_client, job_id = create_batch_job()
        create_and_submit_tasks(batch_client, job_id, work_items_chunks)
        print(f"Job '{job_id}' created with {len(work_items_chunks)} tasks")
    
    except Exception as e:
        print(f"Error: {e}")

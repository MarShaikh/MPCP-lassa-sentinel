"""
Batch job creator for converting Geo-TIFF COG files to STAC item in Azure Batch
"""
from typing import List, Dict

from src.utils.batch_task_utils import filter_existing_items
from src.utils.azure_batch_utils import (
    create_batch_job_with_pool,
    create_and_submit_tasks_with_config,
    STAC_TASK_CONFIG,
    run_batch_job_workflow
)
from src.utils.azure_storage_utils import get_blob_service_client


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

    blob_service_client = get_blob_service_client()

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
    def construct_stac_path(work_item: Dict) -> str:
        """Construct the expected STAC item blob path from a work item."""
        stac_filename = work_item['filename'].replace('.tif', '.json')
        return f"{work_item['year']}/{stac_filename}"

    return filter_existing_items(
        work_items=work_items,
        container_name="stac-items",
        path_constructor=construct_stac_path,
        ensure_container=True
    )


def create_and_submit_tasks(batch_client, job_id: str, work_items_chunks: List[List[Dict]]):
    """
    Upload task data to Azure Storage and submit tasks with ResourceFiles.

    This is a wrapper around the generic create_and_submit_tasks_with_config
    function, using the STAC-specific configuration.

    Parameters:
        batch_client: Azure Batch client
        job_id (str): Batch job ID
        work_items_chunks (List[List[Dict]]): Chunks of work items to process
    """
    create_and_submit_tasks_with_config(
        batch_client=batch_client,
        job_id=job_id,
        work_items_chunks=work_items_chunks,
        config=STAC_TASK_CONFIG
    )


def main():
    """
    Main function to orchestrate the batch job creation for STAC processing.

    Acquires work items from processed-cogs container and uses the generic
    workflow to create and submit the batch job.
    """
    # Get list of COG files to process
    work_items = get_cog_files_to_process()

    # Use generic workflow orchestrator for filtering, chunking, and job submission
    run_batch_job_workflow(
        work_items=work_items,
        filter_func=filter_existing_stac_items,
        job_pool_name="stac-processing",
        task_config=STAC_TASK_CONFIG,
        chunk_size=100,
        job_name="STAC Processing"
    )


if __name__ == "__main__":
    main()

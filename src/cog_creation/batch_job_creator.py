from typing import List


from src.cog_creation.data_extraction import find_tiff_url
from src.utils.batch_task_utils import filter_existing_items
from src.utils.azure_batch_utils import (
    create_batch_job_with_pool,
    create_and_submit_tasks_with_config,
    CHIRPS_TASK_CONFIG,
    run_batch_job_workflow,
)

def create_batch_job():
    """
    Create a batch job for CHIRPS processing.

    Returns:
        tuple: BatchServiceClient and job_id
    """
    return create_batch_job_with_pool("chirps-processing")


def create_and_submit_tasks(batch_client, job_id, work_items_chunks):
    """
    Uploads task data to Azure Storage and submits tasks with ResourceFiles.

    This is a wrapper around the generic create_and_submit_tasks_with_config
    function, using the CHIRPS-specific configuration.
    """
    create_and_submit_tasks_with_config(
        batch_client=batch_client,
        job_id=job_id,
        work_items_chunks=work_items_chunks,
        config=CHIRPS_TASK_CONFIG
    )

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
    def construct_cog_path(work_item: dict) -> str:
        """Construct the expected COG blob path from a work item."""
        year = work_item['year']
        url = work_item['url']
        year_dir = str(year) + "/"

        # Predict the blob path
        raw_file_name = url.split(year_dir)[1].replace(".gz", "")
        cog_file_name = f"nigeria-cog-{raw_file_name}"
        return f"{year}/{cog_file_name}"

    return filter_existing_items(
        work_items=work_items,
        container_name="processed-cogs",
        path_constructor=construct_cog_path,
        ensure_container=False
    )

def main():
    """
    Main function to orchestrate CHIRPS batch processing job creation.

    Acquires work items from CHIRPS data source and uses the generic
    workflow to create and submit the batch job.
    """
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

    # Use generic workflow orchestrator for filtering, chunking, and job submission
    run_batch_job_workflow(
        work_items=work_items,
        filter_func=filter_existing_work_items,
        job_pool_name="chirps-processing",
        task_config=CHIRPS_TASK_CONFIG,
        chunk_size=550,
        job_name="CHIRPS Processing"
    )


if __name__ == "__main__":
    main()
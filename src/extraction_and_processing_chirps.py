from data_extraction import find_tiff_url
from processing import process_batch_with_progress
from typing import List

if __name__ == "__main__":

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
    
    
    work_items = []
    for data in data_urls:
        for url in data['urls']:
            work_items.append({"year": data['year'], "url": url})
    
    # create chunks of data for processing
    def create_chunks(work_items: List[dict], chunk_size: int = 550):
        """
        Splits a list of work items into smaller chunks for batch processing.

        Parameters
        ----------
        work_items : List[dict]
            A list of dictionaries, where each dictionary represents a work item
            (e.g., containing 'year' and 'url' for a file to process).
        chunk_size : int, optional
            The maximum number of work items to include in each chunk.
            Defaults to 550.

        Returns
        -------
        List[List[dict]]
            A list of lists, where each inner list is a chunk of work items.
        """

        chunks = []
        for i in range(0, len(work_items), chunk_size):
            chunk = work_items[i:i+chunk_size]
            chunks.append(chunk)
        return chunks
    
    work_items_chunks = create_chunks(work_items)

    for task_id, work_items_chunk in enumerate(work_items_chunks[0:1]): # only processes 1 batch or 10 files
        process_batch_with_progress(work_items_chunk, task_id)
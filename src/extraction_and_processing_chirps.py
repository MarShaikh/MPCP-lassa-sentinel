from data_extraction import find_tiff_url
from processing import decompress_convert_to_cog

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
    
    # iterate through all the years, and convert to COGS
    directory = "nigeria_tifs/"
    
    # for parallel workflow, convert to a flat list from the nested data_urls list
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import tqdm

    work_items = []
    failed_files = []

    for data in data_urls:
        for url in data['urls']:
            work_items.append({"year": data['year'], "url": url})
    
    
    # parallel code to speed up extraction and processing of data
    with ThreadPoolExecutor(max_workers=20) as executor:
        # Create futures and map them to work items
        future_to_item = {}
        for item in work_items:
            future = executor.submit(decompress_convert_to_cog, item, directory)
            future_to_item[future] = item
        
        for future in tqdm.tqdm(as_completed(future_to_item.keys()), total=len(future_to_item),  desc="Processing files"):
            work_item = future_to_item[future]
            try:
                future.result() 
            except Exception as e:
                failed_files.append(work_item)
                print(f"Failed: {work_item['url']} - Error: {str(e)}")
    
    print(f"\nCompleted! {len(failed_files)} files failed out of {len(work_items)} total")
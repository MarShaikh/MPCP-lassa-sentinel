from typing import List
import requests
import re
from bs4 import BeautifulSoup

def get_table_from_link(url: str, class_: str) -> List[str]:
    """
    Extract table data from a web page by scraping elements with a specific CSS class.
    
    Parameters
    ----------
    url : str
        The URL of the web page to scrape.
    class_ : str
        The CSS class name to search for within table cells.
        
    Returns
    -------
    List[str]
        A list of BeautifulSoup Tag objects containing the matched table cells.
        
    Notes
    -----
    This function assumes the target table has an id="list" attribute.
    It searches for <td> elements within that table matching the specified class.
    """
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    table_ = soup.find(id = "list")
    list_ = table_.find_all("td", class_=class_)
    return list_

def find_data_storage(url: str, pattern: str) -> float:
    """
    Calculate total storage requirements from size data scraped from a web page.
    
    Parameters
    ----------
    url : str
        The URL of the web page containing size information.
    pattern : str
        Regex pattern parameter (currently unused - function uses hardcoded pattern).
        
    Returns
    -------
    float
        Total storage size converted to megabytes (MB).
        
    Notes
    -----
    The function searches for table cells with class="size", extracts numeric values
    from text matching the pattern numbers with decimal points, and
    sums them. The conversion factor 0.001024 is applied, suggesting conversion
    from KiB to MB using binary conversion (1024 bytes per KiB, then /1000).
    """
    storage_list = get_table_from_link(url, class_="size")
    
    total_storage = 0
    for itr in storage_list:
        pattern = re.compile(pattern)
        if pattern.match(itr.text):
            storage_per_file = float(itr.text.split(" ")[0])
            total_storage += storage_per_file

    return total_storage * 0.001024 # converting to MB


def find_tiff_url(url: str, pattern: str) -> List[str]:
    """
    Extract and construct URLs matching a specified pattern from a web page.
    
    Parameters
    ----------
    url : str
        The base URL of the web page to scrape.
    pattern : str
        Regex pattern to match against href attributes in links.
        
    Returns
    -------
    List[str]
        A list of complete URLs constructed by combining the base URL
        with matching href values.
        
    Notes
    -----
    The function searches for table cells with class="link", extracts href
    attributes from anchor tags within those cells, and filters them using
    the provided regex pattern. Complete URLs are formed by concatenating
    the base URL with the matching href values.
    
    Assumes each link cell contains at least one anchor tag with an href attribute.
    """
    links = get_table_from_link(url, class_ = "link")

    all_url = []
    for link in links:
        temp_url = link.find_all(href = True)[0]['href']
        pattern = re.compile(pattern)
        if pattern.match(temp_url):
            all_url.append(url + temp_url)

    return all_url
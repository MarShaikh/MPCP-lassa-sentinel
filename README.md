# Planetary Computer GeoCatalog Ingestion

This Python script facilitates the automated ingestion of geospatial data from Microsoft's Planetary Computer into a custom Azure GeoCatalog. It streamlines the process of data discovery, preparation, and bulk uploading, complete with validation and monitoring capabilities.

## Overview

The script automates the following key steps:

*   **GeoCatalog Setup**: Creates a new STAC collection in your Azure GeoCatalog instance.
*   **Thumbnail Upload**: Attaches a thumbnail to the newly created collection.
*   **Planetary Computer Query**: Searches the Planetary Computer for STAC items based on user-defined criteria (collection, bounding box, date range).
*   **STAC Item Preparation**: Validates retrieved STAC items and automatically corrects common STAC Classification Extension errors by generating missing `name` fields.
*   **Batch Ingestion**: Uploads the validated STAC items in optimized batches to your GeoCatalog collection.
*   **Monitoring & Verification**: Tracks the status of ongoing ingestion operations and verifies the final count of ingested items.

## Prerequisites

Before running the script, ensure you have:

*   An Azure subscription with an active Azure GeoCatalog instance.
*   [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) installed and logged in to an account with permissions to your GeoCatalog.
*   A Python environment with the following packages installed:
    *   `requests`
    *   `azure-identity`
    *   `pystac-client`
    *   `planetary-computer`
    *   `Pillow` (PIL)

## Configuration

Modify the variables within the `if __name__ == "__main__":` block in the `ingestion_to_planetary_computer.py` file to suit your needs:

*   `geocatalog_url`: The base URL of your Azure GeoCatalog.
*   `pc_collection`: The ID of the Planetary Computer collection to ingest (e.g., `"modis-13Q1-061"`).
*   `bbox_aoi`: A list defining the bounding box `[min_lon, min_lat, max_lon, max_lat]` for your Area of Interest.
*   `param_date_range`: The date range string (e.g., `"YYYY-MM-DD/YYYY-MM-DD"`) for filtering items.
*   **Planetary Computer Subscription Key**: Update the placeholder `{{ 53a56e94-45e0-484f-95b7-676b45b31295 }}` in the `planetary_computer.set_subscription_key()` call with your actual key.

## Usage

To run the ingestion:

1.  Open your terminal or command prompt.
2.  Navigate to the directory containing `ingestion_to_planetary_computer.py`.
3.  Execute the script:
    ```bash
    python ingestion_to_planetary_computer.py
    ```

The script will output progress, including collection creation, batch ingestion statuses, and a final verification of the ingested items.
"""
Bulk ingestion of STAC items from Azure Blob Storage catalog into GeoCatalog.

This script creates a STAC collection in the GeoCatalog and ingests items
from a pre-generated catalog.json file stored in Azure Blob Storage.

python src/stac_creation/bulk_ingestion_stac_items.py \
    --geocatalog-uri <geocatalog_uri> \
    --catalog-href <catalog_href> \
    --collection-id <collection_id> \
    --collection-title <collection_title> \
    --collection-desc <collection_desc> \
    --bbox <min_lon> <min_lat> <max_lon> <max_lat> \
    --start-date <start_date_of_collection> \
    --skip-existing-items
"""

import sys
import argparse
from datetime import datetime, timezone
from typing import Optional

import requests
import pystac
from azure.identity import AzureCliCredential


def get_access_token(app_id: str) -> str:
    """
    Obtain an Azure access token using CLI credentials.

    Args:
        app_id: The application ID for token scope

    Returns:
        str: Access token string
    """
    credential = AzureCliCredential()
    access_token = credential.get_token(f"{app_id}/.default")
    return access_token.token


def create_collection_chirps(
    geocatalog_uri: str,
    api_version: str,
    access_token: str,
    collection_id: str,
    collection_title: str,
    collection_desc: str,
    bbox: list,
    start_datetime: datetime,
    skip_if_exists: bool = True
) -> bool:
    """
    Create a CHIRPS-specific STAC collection in the GeoCatalog.

    Args:
        geocatalog_uri: GeoCatalog base URL
        api_version: API version to use
        access_token: Azure access token
        collection_id: Unique collection identifier
        collection_title: Collection title
        collection_desc: Collection description
        bbox: Bounding box [min_lon, min_lat, max_lon, max_lat]
        start_datetime: Start datetime for temporal extent
        skip_if_exists: If True, skip creation if collection already exists

    Returns:
        bool: True if collection was created or already exists, False on error
    """
    # Create spatial extent
    spatial_extent = pystac.SpatialExtent([bbox])

    # Create temporal extent
    temporal_extent = pystac.TemporalExtent([[start_datetime, None]])
    extent = pystac.Extent(spatial=spatial_extent, temporal=temporal_extent)

    # Create the STAC Collection
    collection = pystac.Collection(
        id=collection_id,
        description=collection_desc,
        extent=extent,
        title=collection_title,
        license="private",
    )

    # Add CHIRPS-specific keywords and provider
    collection.keywords = ["CHIRPS", "satellite", "weather", "UCSB"]
    collection.providers = [
        pystac.Provider(
            name="UCSB",
            roles=["producer", "licensor"],
            url="https://data.chc.ucsb.edu"
        )
    ]

    collection_dict = collection.to_dict()
    collection_dict['stac_version'] = '1.0.0'

    # CHIRPS-specific item assets configuration
    collection_dict['item_assets'] = {
        "data": {
            "type": "image/tiff; application=geotiff",
            "roles": ["data"],
            "title": "CHIRPS Precipitation Data",
            "raster:bands": [
                {
                    "data_type": "float32",
                    "nodata": -9999,
                    "unit": "mm"
                }
            ]
        }
    }

    # Create collection via API
    response = requests.post(
        f"{geocatalog_uri}/stac/collections",
        json=collection_dict,
        headers={"Authorization": f"Bearer {access_token}"},
        params={"api-version": api_version},
    )

    if response.status_code in [200, 201]:
        print(f"✓ Collection '{collection_id}' created successfully")
        return True
    elif response.status_code == 409 or "already exists" in response.text.lower():
        if skip_if_exists:
            print(f"✓ Collection '{collection_id}' already exists (skipping)")
            return True
        else:
            print(f"✗ Collection '{collection_id}' already exists")
            print(f"  Response: {response.text}")
            return False
    else:
        print(f"✗ Failed to create collection: {response.status_code}")
        print(f"  Response: {response.text}")
        return False


def create_ingestion(
    geocatalog_uri: str,
    api_version: str,
    access_token: str,
    collection_id: str,
    catalog_href: str,
    skip_existing_items: bool = False,
    keep_original_assets: bool = False,
    timeout_seconds: int = 300
) -> Optional[str]:
    """
    Create an ingestion job for the collection.

    Args:
        geocatalog_uri: GeoCatalog base URL
        api_version: API version to use
        access_token: Azure access token
        collection_id: Collection to ingest into
        catalog_href: URL to the catalog.json file
        skip_existing_items: If True, skip items that already exist
        keep_original_assets: If True, keep original asset URLs
        timeout_seconds: Request timeout in seconds

    Returns:
        Optional[str]: Ingestion ID if successful, None otherwise
    """
    url = f"{geocatalog_uri}/inma/collections/{collection_id}/ingestions"
    body = {
        "importType": "StaticCatalog",
        "sourceCatalogUrl": catalog_href,
        "skipExistingItems": skip_existing_items,
        "keepOriginalAssets": keep_original_assets,
    }

    response = requests.post(
        url,
        json=body,
        timeout=timeout_seconds,
        headers={"Authorization": f"Bearer {access_token}"},
        params={"api-version": api_version},
    )

    if response.status_code == 201:
        ingestion_id = response.json()["id"]
        print(f"✓ Ingestion created successfully")
        print(f"  Ingestion ID: {ingestion_id}")
        return ingestion_id
    else:
        print(f"✗ Failed to create ingestion: {response.status_code}")
        print(f"  Response: {response.text}")
        return None


def start_ingestion_workflow(
    geocatalog_uri: str,
    api_version: str,
    access_token: str,
    collection_id: str,
    ingestion_id: str
) -> bool:
    """
    Start the ingestion workflow/run.

    Args:
        geocatalog_uri: GeoCatalog base URL
        api_version: API version to use
        access_token: Azure access token
        collection_id: Collection ID
        ingestion_id: Ingestion ID

    Returns:
        bool: True if workflow started successfully, False otherwise
    """
    runs_endpoint = (
        f"{geocatalog_uri}/inma/collections/{collection_id}/ingestions/{ingestion_id}/runs"
    )

    response = requests.post(
        runs_endpoint,
        headers={"Authorization": f"Bearer {access_token}"},
        params={"api-version": api_version},
    )

    if response.status_code == 201:
        print(f"✓ Ingestion workflow started successfully")
        return True
    else:
        print(f"✗ Failed to start ingestion workflow: {response.status_code}")
        print(f"  Response: {response.text}")
        return False


def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Bulk ingestion of STAC items from Azure Blob Storage catalog into GeoCatalog",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--geocatalog-uri",
        type=str,
        required=True,
        help="GeoCatalog base URL"
    )

    parser.add_argument(
        "--catalog-href",
        type=str,
        required=True,
        help="URL to the catalog.json file in blob storage"
    )

    parser.add_argument(
        "--collection-id",
        type=str,
        required=True,
        help="Collection ID for the STAC collection"
    )

    parser.add_argument(
        "--collection-title",
        type=str,
        required=True,
        help="Collection title"
    )

    parser.add_argument(
        "--collection-desc",
        type=str,
        required=True,
        help="Collection description"
    )

    parser.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"),
        required=True,
        help="Bounding box [min_lon, min_lat, max_lon, max_lat]"
    )

    parser.add_argument(
        "--start-date",
        type=str,
        default="1981-01-01",
        help="Start date for temporal extent (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--api-version",
        type=str,
        default="2025-04-30-preview",
        help="GeoCatalog API version"
    )

    parser.add_argument(
        "--mpc-app-id",
        type=str,
        default="https://geocatalog.spatio.azure.com",
        help="Microsoft Planetary Computer application ID for authentication"
    )

    parser.add_argument(
        "--skip-existing-items",
        action="store_true",
        help="Skip items that already exist in the collection"
    )

    parser.add_argument(
        "--keep-original-assets",
        action="store_true",
        help="Keep original asset URLs instead of copying to GeoCatalog storage"
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Request timeout in seconds"
    )

    parser.add_argument(
        "--skip-if-collection-exists",
        action="store_true",
        default=True,
        help="Skip collection creation if it already exists"
    )

    return parser.parse_args()


def main():
    """
    Main function to orchestrate the bulk ingestion process.
    """
    args = parse_arguments()

    print("=" * 60)
    print("STAC BULK INGESTION TO GEOCATALOG")
    print("=" * 60)
    print(f"Collection ID: {args.collection_id}")
    print(f"Catalog URL: {args.catalog_href}")
    print(f"GeoCatalog: {args.geocatalog_uri}")
    print("=" * 60)

    # Get access token
    print("\nStep 1: Obtaining access token...")
    try:
        access_token = get_access_token(args.mpc_app_id)
        print("✓ Access token obtained")
    except Exception as e:
        print(f"✗ Failed to obtain access token: {e}")
        sys.exit(1)

    # Parse start date
    try:
        start_datetime = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as e:
        print(f"✗ Invalid start date format: {e}")
        sys.exit(1)

    # Create CHIRPS collection
    print("\nStep 2: Creating CHIRPS STAC collection...")
    success = create_collection_chirps(
        geocatalog_uri=args.geocatalog_uri,
        api_version=args.api_version,
        access_token=access_token,
        collection_id=args.collection_id,
        collection_title=args.collection_title,
        collection_desc=args.collection_desc,
        bbox=args.bbox,
        start_datetime=start_datetime,
        skip_if_exists=args.skip_if_collection_exists
    )

    if not success:
        print("\n✗ Collection creation failed")
        sys.exit(1)

    # Create ingestion
    print("\nStep 3: Creating ingestion job...")
    ingestion_id = create_ingestion(
        geocatalog_uri=args.geocatalog_uri,
        api_version=args.api_version,
        access_token=access_token,
        collection_id=args.collection_id,
        catalog_href=args.catalog_href,
        skip_existing_items=args.skip_existing_items,
        keep_original_assets=args.keep_original_assets,
        timeout_seconds=args.timeout
    )

    if not ingestion_id:
        print("\n✗ Ingestion creation failed")
        sys.exit(1)

    # Start ingestion workflow
    print("\nStep 4: Starting ingestion workflow...")
    success = start_ingestion_workflow(
        geocatalog_uri=args.geocatalog_uri,
        api_version=args.api_version,
        access_token=access_token,
        collection_id=args.collection_id,
        ingestion_id=ingestion_id
    )

    if not success:
        print("\n✗ Workflow start failed")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("✅ BULK INGESTION COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print(f"Collection ID: {args.collection_id}")
    print(f"Ingestion ID: {ingestion_id}")
    print("\nThe ingestion workflow is now running in the background.")
    print("Monitor progress in the GeoCatalog portal.")


if __name__ == "__main__":
    main()

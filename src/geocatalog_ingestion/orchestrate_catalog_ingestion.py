"""
Orchestrator for STAC catalog creation and bulk ingestion workflow.

This script orchestrates the complete workflow:
1. Preliminary checks and validation
2. Create catalog.json from STAC items in blob storage
3. Wait for catalog to be available
4. Bulk ingest the catalog into GeoCatalog

Example Usage:
python src/stac_creation/orchestrate_catalog_ingestion.py \
    --geocatalog-uri <geocatalog_uri> \
    --collection-id <collection_id> \
    --collection-title <collection_title> \
    --collection-desc <collection_desc> \
    --bbox <min_lon> <min_lat> <max_lon> <max_lat> \
    --wait-time 30 \
    --skip-existing-items
"""

import os
import sys
import time
import argparse
import subprocess
from datetime import datetime

from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient


def validate_environment():
    """
    Validate that required environment variables are set.

    Returns:
        bool: True if environment is valid, False otherwise
    """
    required_vars = ["STORAGE_ACCOUNT_URL", "CONTAINER_NAME"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False

    print("Environment variables validated")
    return True


def check_blob_container_exists(container_name: str) -> bool:
    """
    Check if the blob container exists and has STAC items.

    Args:
        container_name: Name of the container to check

    Returns:
        bool: True if container exists and has items, False otherwise
    """
    try:
        storage_account_url = os.environ["STORAGE_ACCOUNT_URL"]
        storage_account_name = storage_account_url.split("//")[1].split(".")[0]
        account_url = f"https://{storage_account_name}.blob.core.windows.net"

        credential = AzureCliCredential()
        blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=credential
        )

        container_client = blob_service_client.get_container_client(container_name)

        # Check if container exists
        if not container_client.exists():
            print(f"Container '{container_name}' does not exist")
            return False

        # Check if container has any JSON files
        blob_count = 0
        for blob in container_client.list_blobs():
            if blob.name.endswith('.json'):
                blob_count += 1
                if blob_count >= 1:  # Just need to confirm at least one exists
                    break

        if blob_count == 0:
            print(f"Container '{container_name}' has no JSON files")
            return False

        print(f"Container '{container_name}' exists and contains STAC items")
        return True

    except Exception as e:
        print(f"Error checking container: {e}")
        return False


def run_catalog_definition():
    """
    Run the define_catalog.py script to create catalog.json.

    Returns:
        bool: True if successful, False otherwise
    """
    print("\n" + "=" * 60)
    print("STEP 1: Creating catalog.json from STAC items")
    print("=" * 60)

    script_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "define_catalog.py"
    )

    if not os.path.exists(script_path):
        print(f"Script not found: {script_path}")
        return False

    try:
        result = subprocess.run(
            ["python", script_path],
            capture_output=True,
            text=True,
            check=True
        )

        print(result.stdout)

        if result.stderr:
            print("Warnings/Errors:")
            print(result.stderr)

        print("Catalog creation completed successfully")
        return True

    except subprocess.CalledProcessError as e:
        print(f"Catalog creation failed with exit code {e.returncode}")
        print(f"Output: {e.stdout}")
        print(f"Error: {e.stderr}")
        return False
    except Exception as e:
        print(f"Unexpected error running catalog creation: {e}")
        return False


def verify_catalog_exists(container_name: str, catalog_filename: str = "catalog.json") -> bool:
    """
    Verify that catalog.json was created successfully.

    Args:
        container_name: Name of the container
        catalog_filename: Name of the catalog file

    Returns:
        bool: True if catalog exists, False otherwise
    """
    try:
        storage_account_url = os.environ["STORAGE_ACCOUNT_URL"]
        storage_account_name = storage_account_url.split("//")[1].split(".")[0]
        account_url = f"https://{storage_account_name}.blob.core.windows.net"

        credential = AzureCliCredential()
        blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=credential
        )

        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=catalog_filename
        )

        if blob_client.exists():
            print(f"Catalog file '{catalog_filename}' exists in container '{container_name}'")
            return True
        else:
            print(f"Catalog file '{catalog_filename}' not found in container '{container_name}'")
            return False

    except Exception as e:
        print(f"Error verifying catalog: {e}")
        return False


def run_bulk_ingestion(
    geocatalog_uri: str,
    catalog_href: str,
    collection_id: str,
    collection_title: str,
    collection_desc: str,
    bbox: list,
    start_date: str,
    api_version: str,
    mpc_app_id: str,
    skip_existing_items: bool,
    keep_original_assets: bool
) -> bool:
    """
    Run the bulk ingestion script.

    Returns:
        bool: True if successful, False otherwise
    """
    print("\n" + "=" * 60)
    print("STEP 3: Running bulk ingestion")
    print("=" * 60)

    script_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "bulk_ingestion_stac_items.py"
    )

    if not os.path.exists(script_path):
        print(f"✗ Script not found: {script_path}")
        return False

    # Build command
    cmd = [
        "python", script_path,
        "--geocatalog-uri", geocatalog_uri,
        "--catalog-href", catalog_href,
        "--collection-id", collection_id,
        "--collection-title", collection_title,
        "--collection-desc", collection_desc,
        "--bbox", *[str(b) for b in bbox],
        "--start-date", start_date,
        "--api-version", api_version,
        "--mpc-app-id", mpc_app_id,
    ]

    if skip_existing_items:
        cmd.append("--skip-existing-items")

    if keep_original_assets:
        cmd.append("--keep-original-assets")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )

        print(result.stdout)

        if result.stderr:
            print("Warnings/Errors:")
            print(result.stderr)

        print("✓ Bulk ingestion completed successfully")
        return True

    except subprocess.CalledProcessError as e:
        print(f"✗ Bulk ingestion failed with exit code {e.returncode}")
        print(f"Output: {e.stdout}")
        print(f"Error: {e.stderr}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error running bulk ingestion: {e}")
        return False


def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Orchestrate STAC catalog creation and bulk ingestion workflow",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--geocatalog-uri",
        type=str,
        required=True,
        help="GeoCatalog base URL"
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
        "--wait-time",
        type=int,
        default=30,
        help="Wait time (in seconds) between catalog creation and ingestion"
    )

    parser.add_argument(
        "--catalog-filename",
        type=str,
        default="catalog.json",
        help="Name of the catalog file in blob storage"
    )

    return parser.parse_args()


def main():
    """
    Main orchestration function.
    """
    args = parse_arguments()

    print("=" * 60)
    print("STAC CATALOG CREATION AND INGESTION ORCHESTRATOR")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Collection ID: {args.collection_id}")
    print(f"GeoCatalog: {args.geocatalog_uri}")
    print("=" * 60)

    # Preliminary checks
    print("\nPreliminary checks...")
    print("-" * 60)

    # Check environment variables
    if not validate_environment():
        print("\nEnvironment validation failed")
        sys.exit(1)

    # Get container name from environment
    container_name = os.environ["CONTAINER_NAME"]

    # Check blob container exists and has data
    if not check_blob_container_exists(container_name):
        print("\nContainer validation failed")
        sys.exit(1)

    print("\nAll preliminary checks passed")

    # Step 1: Create catalog.json
    if not run_catalog_definition():
        print("\nCatalog creation failed")
        sys.exit(1)

    # Verify catalog was created
    if not verify_catalog_exists(container_name, args.catalog_filename):
        print("\nCatalog verification failed")
        sys.exit(1)

    # Step 2: Wait for catalog to be available
    print("\n" + "=" * 60)
    print(f"STEP 2: Waiting {args.wait_time} seconds for catalog availability")
    print("=" * 60)
    print(f"Waiting to ensure catalog is fully propagated...")

    for remaining in range(args.wait_time, 0, -1):
        print(f"  {remaining} seconds remaining...", end="\r")
        time.sleep(1)

    print("\nWait completed")

    # Build catalog URL
    storage_account_url = os.environ["STORAGE_ACCOUNT_URL"]
    storage_account_name = storage_account_url.split("//")[1].split(".")[0]
    catalog_href = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{args.catalog_filename}"

    print(f"\nCatalog URL: {catalog_href}")

    # Step 3: Run bulk ingestion
    if not run_bulk_ingestion(
        geocatalog_uri=args.geocatalog_uri,
        catalog_href=catalog_href,
        collection_id=args.collection_id,
        collection_title=args.collection_title,
        collection_desc=args.collection_desc,
        bbox=args.bbox,
        start_date=args.start_date,
        api_version=args.api_version,
        mpc_app_id=args.mpc_app_id,
        skip_existing_items=args.skip_existing_items,
        keep_original_assets=args.keep_original_assets
    ):
        print("\nBulk ingestion failed")
        sys.exit(1)

    # Success
    print("\n" + "=" * 60)
    print("ORCHESTRATION COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Collection ID: {args.collection_id}")
    print(f"Catalog URL: {catalog_href}")
    print("\nThe ingestion workflow is now running in the background.")
    print("Monitor progress in the GeoCatalog portal.")


if __name__ == "__main__":
    main()

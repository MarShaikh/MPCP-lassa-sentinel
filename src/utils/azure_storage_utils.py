"""
Azure Storage utility functions for batch processing.
Handles SAS token generation and blob operations.
"""
import os
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential


def upload_blob_to_azure_with_sas(
    storage_account_url: str,
    container_name: str,
    file_path: str,
    file_name: str,
    sas_token: str
) -> None:
    """
    Upload a local file to Azure Blob Storage using SAS token.
    
    Parameters:
        storage_account_url (str): Storage account URL
        container_name (str): Target container name
        file_path (str): Local file path
        file_name (str): Blob name in container
        sas_token (str): SAS token for authentication
    """
    blob_service_client = BlobServiceClient(
        account_url=f"{storage_account_url}?{sas_token}"
    )
    
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=file_name
    )
    
    print(f"\nUploading to Azure as blob:\n\t{file_path}")
    with open(file=file_path, mode="rb") as data:
        blob_client.upload_blob(data, overwrite=True)


def get_blob_service_client() -> BlobServiceClient:
    """
    Create and return an authenticated BlobServiceClient.

    Reads STORAGE_ACCOUNT_URL from environment variables and uses appropriate
    authentication based on the URL protocol:
    - HTTPS: Uses DefaultAzureCredential (for Azure)
    - HTTP: Uses connection string (for Azurite local testing)

    This consolidates the repeated BlobServiceClient setup pattern across the codebase
    while supporting both production (Azure) and local testing (Azurite) environments.

    Returns:
        BlobServiceClient: Authenticated blob service client

    Raises:
        KeyError: If STORAGE_ACCOUNT_URL environment variable is not set
    """
    storage_account_url = os.environ["STORAGE_ACCOUNT_URL"]

    # Use DefaultAzureCredential for HTTPS or connection string for HTTP (Azurite)
    if storage_account_url.startswith("https://"):
        storage_credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=storage_account_url,
            credential=storage_credential
        )
    else:
        # For Azurite or HTTP, try connection string first
        connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if connection_string:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        else:
            # Fall back to account URL without credential (will fail if auth is required)
            blob_service_client = BlobServiceClient(account_url=storage_account_url)

    return blob_service_client


def ensure_container_exists(blob_service_client: BlobServiceClient, container_name: str) -> None:
    """
    Check if a container exists and create it if it doesn't.

    This consolidates the repeated container check-and-create pattern used
    across the codebase.

    Parameters:
        blob_service_client (BlobServiceClient): Authenticated blob service client
        container_name (str): Name of the container to check/create

    Note:
        Silently succeeds if the container already exists. Prints a message
        only when creating a new container.
    """
    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.get_container_properties()
    except:
        container_client.create_container()
        print(f"Created {container_name} container")
"""
Azure Storage utility functions for batch processing.
Handles SAS token generation and blob operations.
"""
from azure.storage.blob import (
    BlobServiceClient
)


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
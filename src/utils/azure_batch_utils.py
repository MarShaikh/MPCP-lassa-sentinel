"""
Shared utilities for Azure Batch operations.
Consolidates duplicate code from job creators.
"""
import os
import json
from datetime import datetime, timedelta, timezone
from typing import Tuple, List, Dict

from azure.batch import BatchServiceClient
from azure.batch.models import JobAddParameter, PoolInformation, TaskAddParameter, ResourceFile
from azure.common.credentials import ServicePrincipalCredentials
from azure.storage.blob import (
    BlobServiceClient, generate_blob_sas, generate_container_sas,
    BlobSasPermissions, ContainerSasPermissions
)
from azure.identity import DefaultAzureCredential


def get_batch_credentials() -> ServicePrincipalCredentials:
    """
    Get Azure Batch service principal credentials from environment variables.

    Returns:
        ServicePrincipalCredentials: Authenticated credentials
    """
    TENANT_ID = os.environ["AZURE_TENANT_ID"]
    CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
    CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
    RESOURCE = "https://batch.core.windows.net/"

    credentials = ServicePrincipalCredentials(
        client_id=CLIENT_ID,
        secret=CLIENT_SECRET,
        tenant=TENANT_ID,
        resource=RESOURCE
    )

    return credentials


def create_batch_client() -> BatchServiceClient:
    """
    Create an authenticated Azure Batch client.

    Returns:
        BatchServiceClient: Authenticated Batch client
    """
    BATCH_ACCOUNT_URL = os.environ["BATCH_ACCOUNT_URL"]
    credentials = get_batch_credentials()

    batch_client = BatchServiceClient(
        credentials,
        batch_url=BATCH_ACCOUNT_URL
    )

    return batch_client


def create_batch_job_with_pool(job_type: str, pool_id: str = "geospatial-processing-pool") -> Tuple[BatchServiceClient, str]:
    """
    Create a batch job with specified job type and pool.

    Parameters:
        job_type (str): Type of job (e.g., "stac-processing", "chirps-processing")
        pool_id (str): Pool ID to use for the job. Defaults to "geospatial-processing-pool"

    Returns:
        tuple: (BatchServiceClient, job_id)
    """
    batch_client = create_batch_client()

    # Create a unique job ID with timestamp
    job_id = f"{job_type}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    # Create job configuration
    job = JobAddParameter(
        id=job_id,
        pool_info=PoolInformation(pool_id=pool_id)
    )

    # Create the job
    print(f"Creating job: {job_id}")
    batch_client.job.add(job)

    return batch_client, job_id


def generate_sas_tokens_for_containers(container_configs: Dict[str, Dict], expiry_days: int = 7) -> Dict[str, str]:
    """
    Generate SAS tokens for multiple Azure Storage containers.

    Parameters:
        container_configs (Dict[str, Dict]): Dictionary mapping container names to their permission configs
            Example: {
                "processed-cogs": {"read": True, "write": True},
                "stac-items": {"read": True, "write": True, "create": True, "list": True}
            }
        expiry_days (int): Number of days until SAS token expires. Defaults to 7.

    Returns:
        Dict[str, str]: Dictionary mapping container names to their SAS tokens
    """
    STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
    STORAGE_ACCOUNT_NAME = STORAGE_ACCOUNT_URL.split("//")[1].split(".")[0]
    BATCH_STORAGE_ACCOUNT_KEY = os.environ["BATCH_STORAGE_ACCOUNT_KEY"]

    sas_tokens = {}
    expiry = datetime.now(timezone.utc) + timedelta(days=expiry_days)

    for container_name, permissions in container_configs.items():
        # Build permission object from config
        permission = ContainerSasPermissions(
            read=permissions.get("read", False),
            write=permissions.get("write", False),
            delete=permissions.get("delete", False),
            list=permissions.get("list", False),
            create=permissions.get("create", False)
        )

        sas_token = generate_container_sas(
            account_name=STORAGE_ACCOUNT_NAME,
            container_name=container_name,
            account_key=BATCH_STORAGE_ACCOUNT_KEY,
            permission=permission,
            expiry=expiry
        )
        sas_tokens[container_name] = sas_token

    return sas_tokens


def create_task_data_blob_client(ensure_container: bool = False, use_default_credential: bool = True) -> Tuple[BlobServiceClient, str]:
    """
    Create a blob service client for the task-data container.

    Parameters:
        ensure_container (bool): If True, creates the container if it doesn't exist
        use_default_credential (bool): If True, use DefaultAzureCredential. If False, uses connection string or account key from env.

    Returns:
        Tuple[BlobServiceClient, str]: (BlobServiceClient, container_name)
    """
    STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
    CONTAINER_NAME = "task-data"

    # Use DefaultAzureCredential for HTTPS or connection string for HTTP (Azurite)
    if use_default_credential and STORAGE_ACCOUNT_URL.startswith("https://"):
        storage_credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=STORAGE_ACCOUNT_URL,
            credential=storage_credential
        )
    else:
        # For Azurite or when not using default credential, try connection string first
        connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if connection_string:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        else:
            # Fall back to account URL without credential (will fail if auth is required)
            blob_service_client = BlobServiceClient(account_url=STORAGE_ACCOUNT_URL)

    if ensure_container:
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        try:
            container_client.get_container_properties()
        except:
            container_client.create_container()
            print(f"Created {CONTAINER_NAME} container")

    return blob_service_client, CONTAINER_NAME


def upload_work_items_and_create_task(
    blob_service_client: BlobServiceClient,
    container_name: str,
    job_id: str,
    task_id: str,
    work_items_chunk: List[Dict],
    env_vars: Dict[str, str],
    script_path: str,
    expiry_days: int = 7
) -> TaskAddParameter:
    """
    Upload work items to blob storage and create a Batch task.

    Parameters:
        blob_service_client: Azure Blob Service Client
        container_name: Name of the container for task data
        job_id: Batch job ID
        task_id: Task ID
        work_items_chunk: Chunk of work items for this task
        env_vars: Environment variables to set (e.g., {"STORAGE_ACCOUNT_URL": "...", "COG_CONTAINER_SAS": "..."})
        script_path: Path to the Python script to run (e.g., "src/batch_stac_task_runner.py")
        expiry_days: Days until blob SAS expires

    Returns:
        TaskAddParameter: Task ready to be submitted
    """
    STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
    STORAGE_ACCOUNT_NAME = STORAGE_ACCOUNT_URL.split("//")[1].split(".")[0]
    BATCH_STORAGE_ACCOUNT_KEY = os.environ["BATCH_STORAGE_ACCOUNT_KEY"]

    # Upload work items to blob
    work_items_json = json.dumps(work_items_chunk)
    blob_name = f"{job_id}/{task_id}_work_items.json"

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )
    blob_client.upload_blob(work_items_json.encode('utf-8'), overwrite=True)
    print(f"Uploaded data for {task_id} to blob: {blob_name}")

    # Create SAS URL for batch nodes
    sas_token = generate_blob_sas(
        account_name=STORAGE_ACCOUNT_NAME,
        container_name=container_name,
        blob_name=blob_name,
        account_key=BATCH_STORAGE_ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(days=expiry_days)
    )
    blob_sas_url = f"{blob_client.url}?{sas_token}"

    # Create resource file
    resource_file = ResourceFile(
        http_url=blob_sas_url,
        file_path="work_items.json"
    )

    # Build environment variable exports for command line
    env_exports = " && ".join([f'export {key}="{value}"' for key, value in env_vars.items()])

    # Command line to run on batch node
    command_line = (
        "/bin/bash -c '"
        f"{env_exports} && "
        "cd /tmp && "
        "[ -d code ] && rm -rf code; "
        "git clone https://github.com/MarShaikh/MPCP-lassa-sentinel.git code && "
        "cd code && "
        "python3.11 -m pip install -r requirements.txt && "
        f"python3.11 {script_path}'"
    )

    task = TaskAddParameter(
        id=task_id,
        command_line=command_line,
        resource_files=[resource_file]
    )

    return task

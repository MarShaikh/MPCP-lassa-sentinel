"""
Shared utilities for Azure Batch operations.
Consolidates duplicate code from job creators.
"""
import os
import json
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Tuple, List, Dict, Callable

from azure.batch import BatchServiceClient
from azure.batch.models import JobAddParameter, PoolInformation, TaskAddParameter, ResourceFile
from azure.batch.custom.custom_errors import CreateTasksErrorException
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


def handle_batch_task_creation_error(error: CreateTasksErrorException) -> None:
    """
    Handle CreateTasksErrorException by printing detailed error information.

    This utility consolidates duplicate error handling logic from job creators.

    Parameters:
        error (CreateTasksErrorException): The exception from batch task creation
    """
    print("An error occurred while adding tasks.")
    print("Printing details for each failed task...")

    for failure in error.failure_tasks:
        print(f"  - Task ID: {failure.task_id}")
        print(f"    - Error Code: {failure.error.code}")
        print(f"    - Error Message: {failure.error.message}")

        if failure.error.values:
            for detail in failure.error.values:
                print(f"      - Detail Key: {detail.key}, Value: {detail.value}")

    traceback.print_exc()


@dataclass
class BatchTaskConfig:
    """
    Configuration for batch task creation and submission.

    This dataclass encapsulates the differences between job types (CHIRPS, STAC, etc.)
    to enable code reuse in task submission logic.
    """
    job_type: str
    script_path: str
    container_configs: Dict[str, Dict[str, bool]]
    env_var_mapping: Dict[str, str]  # Maps container name to env var name
    task_id_prefix: str = "task"
    ensure_task_data_container: bool = False


# Pre-defined configurations for common job types
CHIRPS_TASK_CONFIG = BatchTaskConfig(
    job_type="CHIRPS",
    script_path="src/batch_processing/batch_task_runner.py",
    container_configs={
        "processed-cogs": {"read": True, "write": True, "create": True, "list": True},
        "raw-data": {"read": True, "write": True, "create": True, "list": True},
        "batch-logs": {"read": True, "write": True, "create": True, "list": True}
    },
    env_var_mapping={
        "processed-cogs": "COG_CONTAINER_SAS",
        "raw-data": "RAW_CONTAINER_SAS",
        "batch-logs": "LOGS_CONTAINER_SAS"
    },
    task_id_prefix="task",
    ensure_task_data_container=False
)

STAC_TASK_CONFIG = BatchTaskConfig(
    job_type="STAC",
    script_path="src/batch_stac_task_runner.py",
    container_configs={
        "processed-cogs": {"read": True, "list": True},
        "stac-items": {"read": True, "write": True, "create": True, "list": True},
        "batch-logs": {"read": True, "write": True, "create": True, "list": True}
    },
    env_var_mapping={
        "processed-cogs": "COG_CONTAINER_SAS",
        "stac-items": "STAC_CONTAINER_SAS",
        "batch-logs": "LOGS_CONTAINER_SAS"
    },
    task_id_prefix="stac_task",
    ensure_task_data_container=True
)


def create_and_submit_tasks_with_config(
    batch_client,
    job_id: str,
    work_items_chunks: List[List[Dict]],
    config: BatchTaskConfig
) -> None:
    """
    Generic function to create and submit tasks with configuration.

    This consolidates the duplicate create_and_submit_tasks logic from both
    job creators by using a configuration object to specify job-type-specific
    behavior.

    Parameters:
        batch_client: Azure Batch service client
        job_id (str): Batch job ID
        work_items_chunks (List[List[Dict]]): Chunks of work items to process
        config (BatchTaskConfig): Configuration specifying job-type-specific behavior
    """
    # Setup Azure Storage
    blob_service_client, container_name = create_task_data_blob_client(
        ensure_container=config.ensure_task_data_container
    )

    # Generate SAS tokens for containers
    sas_tokens = generate_sas_tokens_for_containers(config.container_configs)

    # Build environment variables using the mapping
    STORAGE_ACCOUNT_URL = os.environ["STORAGE_ACCOUNT_URL"]
    env_vars = {"STORAGE_ACCOUNT_URL": STORAGE_ACCOUNT_URL}

    for container_name_key, env_var_name in config.env_var_mapping.items():
        env_vars[env_var_name] = sas_tokens[container_name_key]

    # Create tasks
    tasks = []
    for i, chunk in enumerate(work_items_chunks):
        task_id = f"{config.task_id_prefix}{i:03d}"

        task = upload_work_items_and_create_task(
            blob_service_client=blob_service_client,
            container_name=container_name,
            job_id=job_id,
            task_id=task_id,
            work_items_chunk=chunk,
            env_vars=env_vars,
            script_path=config.script_path
        )
        tasks.append(task)

    # Submit all tasks at once
    print(f"Submitting {len(tasks)} tasks to job {job_id}")
    batch_client.task.add_collection(job_id, tasks)
    print("All tasks submitted successfully!")


def run_batch_job_workflow(
    work_items: List[Dict],
    filter_func: Callable[[List[Dict]], List[Dict]],
    job_pool_name: str,
    task_config: BatchTaskConfig,
    chunk_size: int = 100,
    job_name: str = "batch job"
) -> None:
    """
    Generic workflow orchestrator for batch job creation and submission.

    This consolidates the common pattern used in main() functions:
    filter items -> create chunks -> create job -> submit tasks -> handle errors

    Parameters:
        work_items (List[Dict]): Initial list of work items to process
        filter_func (Callable): Function to filter out already-processed items
        job_pool_name (str): Name of the batch pool to use
        task_config (BatchTaskConfig): Configuration for task creation
        chunk_size (int): Number of items per task chunk
        job_name (str): Display name for logging

    Raises:
        SystemExit: If no work items remain after filtering or if job creation fails
    """
    from src.utils.batch_task_utils import create_chunks

    # Validate initial work items
    if not work_items:
        print(f"No work items found for {job_name}. Exiting.")
        return

    print(f"Found {len(work_items)} total work items for {job_name}")

    # Filter out already-processed items
    filtered_work_items = filter_func(work_items)

    if not filtered_work_items:
        print(f"All work items already processed for {job_name}! No new job needed.")
        return

    # Create chunks for parallel processing
    work_items_chunks = create_chunks(filtered_work_items, chunk_size=chunk_size)

    print(f"Created {len(work_items_chunks)} chunks to be submitted as tasks.")

    # Create and submit batch job
    try:
        batch_client, job_id = create_batch_job_with_pool(job_pool_name)
        create_and_submit_tasks_with_config(
            batch_client=batch_client,
            job_id=job_id,
            work_items_chunks=work_items_chunks,
            config=task_config
        )
        print(f"Job '{job_id}' created with {len(work_items_chunks)} tasks.")

    except CreateTasksErrorException as e:
        handle_batch_task_creation_error(e)

    except Exception as e:
        print(f"An error occurred during job or task creation: {e}")
        traceback.print_exc()

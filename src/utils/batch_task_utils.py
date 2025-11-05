"""
Utility functions for batch task operations.
Handles work items, directories, progress tracking, and file cleanup.
"""
import os
import json
from typing import List, Dict, Tuple
from datetime import datetime

from azure.storage.blob import BlobServiceClient

def get_work_items_from_file(working_dir: str = None) -> List[Dict]:
    """
    Read work items from a JSON file in the task's working directory.
    
    Parameters:
        working_dir (str, optional): Working directory path. 
                                    If None, uses AZ_BATCH_TASK_WORKING_DIR env var.
    
    Returns:
        list: List of work items to process
    
    Raises:
        FileNotFoundError: If work items file doesn't exist
        ValueError: If JSON parsing fails
    """
    if working_dir is None:
        working_dir = os.environ.get("AZ_BATCH_TASK_WORKING_DIR")
    
    file_path = os.path.join(working_dir, "work_items.json")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"Work items file not found at: {file_path}"
        )
    
    try:
        with open(file_path, 'r') as f:
            work_items = json.load(f)
        return work_items
    except (json.JSONDecodeError, IOError) as e:
        raise ValueError(f"Failed to read or parse work_items.json: {e}")


def setup_working_directories(base_dirs: List[str] = None) -> None:
    """
    Create necessary directories for processing.
    
    Parameters:
        base_dirs (list, optional): List of directory paths to create.
                                   If None, creates default processing directories.
    """
    if base_dirs is None:
        base_dirs = [
            "/tmp/processing/raw-data",
            "/tmp/processing/processed-cogs",
            "/tmp/batch-logs"
        ]
    
    for directory in base_dirs:
        os.makedirs(directory, exist_ok=True)


def create_chunks(work_items: List[Dict], chunk_size: int = 100) -> List[List[Dict]]:
    """
    Split work items into chunks for batch processing.
    
    Parameters:
        work_items (list): List of work items
        chunk_size (int): Number of items per chunk
    
    Returns:
        list: List of chunks (each chunk is a list of work items)
    """
    chunks = []
    for i in range(0, len(work_items), chunk_size):
        chunk = work_items[i:i + chunk_size]
        chunks.append(chunk)
    return chunks


def update_progress_file(
    task_id: str,
    completed: List[Dict],
    failed: List[Dict],
    total: int,
    storage_account_url: str,
    logs_sas: str,
    progress_file_prefix: str = ""
) -> None:
    """
    Update and upload a progress file to Azure Blob Storage.
    
    Parameters:
        task_id (str): Task identifier
        completed (list): List of completed items
        failed (list): List of failed items
        total (int): Total number of items
        storage_account_url (str): Storage account URL
        logs_sas (str): SAS token for logs container
        progress_file_prefix (str): Prefix for progress file name (e.g., 'stac_')
    """
    progress_data = {
        "task_id": task_id,
        "timestamp": datetime.now().isoformat(),
        "total_items": total,
        "completed_count": len(completed),
        "failed_count": len(failed),
        "progress_percentage": (len(completed) / total * 100) if total > 0 else 0,
        "completed_items": completed[-10:] if len(completed) > 10 else completed,
        "failed_items": failed
    }
    
    # Save locally first
    temp_dir = "/tmp/batch-logs"
    os.makedirs(temp_dir, exist_ok=True)
    file_name = f"{progress_file_prefix}{task_id}_progress.json"
    local_file_path = os.path.join(temp_dir, file_name)
    
    with open(local_file_path, 'w') as f:
        json.dump(progress_data, f, indent=2)
    
    # Upload to blob storage
    try:
        blob_service_client = BlobServiceClient(
            account_url=f"{storage_account_url}?{logs_sas}"
        )
        blob_client = blob_service_client.get_blob_client(
            container="batch-logs",
            blob=file_name
        )
        
        with open(local_file_path, 'rb') as f:
            blob_client.upload_blob(f, overwrite=True)
        
        print(f"Progress updated: {len(completed)}/{total} completed")
        
    except Exception as e:
        print(f"Failed to upload progress file: {e}")


def cleanup_local_files(file_paths: List[Tuple] | str) -> None:
    """
    Delete local files after uploading to Azure Blob.
    
    Parameters:
        file_paths: Either a single file path string or list of (file1, file2) tuples
    """
    try:
        if isinstance(file_paths, str):
            os.remove(file_paths)
            print(f"Local {file_paths} removed")
        else:
            for (file1, file2) in file_paths:
                try:
                    os.remove(file1)
                    print(f"Local file removed: {file1}")
                except FileNotFoundError:
                    print(f"File '{file1}' not found.")
                
                try:
                    os.remove(file2)
                    print(f"Local file removed: {file2}")
                except FileNotFoundError:
                    print(f"File '{file2}' not found.")
    except Exception as e:
        print(f"Error during cleanup: {e}")
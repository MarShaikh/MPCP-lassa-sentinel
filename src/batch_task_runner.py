import os
import json
import sys
from typing import List, Dict

from processing import process_batch_with_progress

def get_work_items_from_file() -> List[Dict]:
    """
    Reads work items from a JSON file in the task's working directory.
    The file is downloaded by the Batch service via a ResourceFile.
    
    Returns
    -------
    List[dict]
        List of work items to process
    """
     # AZ_BATCH_TASK_WORKING_DIR is a default environment variable set by Batch.
    task_working_dir = os.environ.get("AZ_BATCH_TASK_WORKING_DIR")
    file_path = os.path.join(task_working_dir, "work_items.json")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Work items file not found at: {file_path}")

    try:
        with open(file_path, 'r') as f:
            work_items = json.load(f)
        return work_items
    except (json.JSONDecodeError, IOError) as e:
        raise ValueError(f"Failed to read or parse work_items.json: {e}")
    
    
def setup_working_directories():
    """
    Creates necessary directories for processing on the VM
    """
    base_dir = "/tmp/processing"
    os.makedirs(os.path.join(base_dir, "raw-data"), exist_ok=True)
    os.makedirs(os.path.join(base_dir, "processed-cogs"), exist_ok=True)
    os.makedirs("/tmp/batch-logs", exist_ok=True)

def main():
    try:
        print("Starting batch task runner...")
        task_id = os.environ.get('AZ_BATCH_TASK_ID', 'unknown_task')
        print(f"Task ID: {task_id}")

        setup_working_directories()
    
        work_items = get_work_items_from_file()
        
        print(f"Processing {len(work_items)} files from resource file.")

        process_batch_with_progress(work_items)

        print(f"Task {task_id} completed successfully")

    except Exception as e:
        print(f"Task failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
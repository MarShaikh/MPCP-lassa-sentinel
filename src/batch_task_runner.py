import os
import json
import sys
from typing import List, Dict

from processing import process_batch_with_progress

def get_work_items_from_environment() -> List[Dict]:
    """
    Retrieves work items from the WORK_ITEMS_JSON environment variable
    and parses them back into a list of dictionaries.
    
    Returns
    -------
    List[dict]
        List of work items to process
    """
    work_items_json = os.environ.get("WORK_ITEMS_JSON")

    if not work_items_json:
        raise ValueError("WORK_ITEMS_JSON environment variable not found.")
    
    try: 
        work_items = json.loads(work_items_json)
        return work_items
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse WORK_ITEMS_JSON: {e}")
    
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

        # get task info
        task_id = os.environ.get('AZ_BATCH_TASK_ID', 'unknown_task')
        print(f"Task ID: {task_id}")

        # setup working directories
        setup_working_directories()

        # get work items
        work_items = get_work_items_from_environment()
        print(f"Processing {len(work_items)} files")

        # Process the batch
        process_batch_with_progress(work_items)

        print(f"Task {task_id} completed successfully")

    except Exception as e:
        print(f"Task failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
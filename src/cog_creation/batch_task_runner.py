import os
import json
import sys
from typing import List, Dict

from src.cog_creation.processing import process_batch_with_progress
from src.utils.batch_task_utils import get_work_items_from_file, setup_working_directories


def main():
    try:
        print("Starting batch task runner...")
        task_id = os.environ.get('AZ_BATCH_TASK_ID', 'unknown_task')
        print(f"Task ID: {task_id}")

        setup_working_directories([
            "/tmp/processing/raw-data",
            "/tmp/processing/processed-cogs",
            "/tmp/batch-logs"
        ])
    
        work_items = get_work_items_from_file()
        
        print(f"Processing {len(work_items)} files from resource file.")

        process_batch_with_progress(work_items)

        print(f"Task {task_id} completed successfully")

    except Exception as e:
        print(f"Task failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
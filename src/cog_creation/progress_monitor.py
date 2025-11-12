import json
import time
from datetime import datetime, timedelta
from typing import Dict, List

from src.utils.base_progress_monitor import BaseProgressMonitor


class ProgressMonitor(BaseProgressMonitor):
    def __init__(self, storage_account_url: str = "https://mpcpstorageaccount.blob.core.windows.net"):
        """
        Initialize the CHIRPS progress monitor.

        Parameters:
            storage_account_url (str): Azure storage account URL (unused, kept for compatibility)
        """
        super().__init__(storage_account_url=storage_account_url, container_name="batch-logs")

    def get_file_prefix(self) -> str:
        """Return the prefix for filtering CHIRPS progress files."""
        return "task"

    def get_job_name(self) -> str:
        """Return the job name for display."""
        return "CHIRPS Processing"

    def extract_task_metrics(self, task: Dict) -> Dict:
        """
        Extract completed, failed, and total counts from a CHIRPS task progress dict.

        Returns:
            Dict with keys: completed, failed, total, task_id
        """
        completed = task.get('completed', 0)
        failed_files = task.get('failed_files', [])
        # CHIRPS uses 550 files per task
        total = 550
        task_id = task.get('batch_number', 'unknown')

        return {
            'completed': completed,
            'failed': len(failed_files),
            'total': total,
            'task_id': task_id
        }

    def get_all_progress_files(self) -> List[Dict]:
        """
        Retrieves all task progress files from blob storage

        Returns
        -------
        List[Dict]
            List of progress data from all tasks
        """
        # Use the base class implementation
        return self.get_progress_files()

    def get_failed_items_report(self) -> List[Dict]:
        """
        Returns detailed report of all failed files across tasks
        """
        progress_data = self.get_all_progress_files()
        failed_files = []

        for task in progress_data:
            task_id = task.get('batch_number', 'unknown')
            for failed_file in task.get('failed_files', []):
                failed_files.append({
                    'task_id': task_id,
                    'file_info': failed_file,
                    'timestamp': task.get('iso_timestamp')
                })

        return failed_files

    # Backward compatibility alias
    def get_failed_files_report(self) -> List[Dict]:
        """Backward compatibility wrapper for get_failed_items_report."""
        return self.get_failed_items_report()

def main():
    monitor = ProgressMonitor()

    # Choose monitoring mode
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--continuous':
        monitor.monitor_continuously()
    elif len(sys.argv) > 1 and sys.argv[1] == '--failed':
        failed_files = monitor.get_failed_items_report()
        print(f"Found {len(failed_files)} failed files:")
        for failure in failed_files:
            print(f"Task {failure['task_id']}: {failure['file_info']}")
    else:
        # Single check
        progress_data = monitor.get_all_progress_files()
        if progress_data:
            summary = monitor.calculate_overall_progress(progress_data)
            monitor.display_progress(summary)
        else:
            print("No progress files found")

if __name__ == "__main__":
    main()
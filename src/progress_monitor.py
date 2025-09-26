# to be run locally
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

class ProgressMonitor:
    def __init__(self, storage_account_url: str = "https://mpcpstorageaccount.blob.core.windows.net"):
        self.credential = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient(
            account_url=storage_account_url,
            credential=self.credential
        )
        self.container_name = "batch-logs"

    def get_all_progress_files(self) -> List[Dict]:
        """
        Retrieves all task progress files from blob storage
        
        Returns
        -------
        List[Dict]
            List of progress data from all tasks
        """
        progress_data = []
        
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blob_list = container_client.list_blobs(name_starts_with="task_")
            
            for blob in blob_list:
                if blob.name.endswith('.json'):
                    blob_client = container_client.get_blob_client(blob.name)
                    content = blob_client.download_blob().readall()
                    task_data = json.loads(content)
                    progress_data.append(task_data)
                    
        except Exception as e:
            print(f"Error reading progress files: {e}")
            
        return progress_data

    def calculate_overall_progress(self, progress_data: List[Dict]) -> Dict:
        """
        Aggregates progress data from all tasks
        
        Parameters
        ----------
        progress_data : List[Dict]
            List of task progress data
            
        Returns
        -------
        Dict
            Overall progress summary
        """
        total_completed = 0
        total_failed = 0
        active_tasks = 0
        completed_tasks = 0
        failed_tasks = 0
        stuck_tasks = []
        
        current_time = datetime.now()
        
        for task in progress_data:
            total_completed += task.get('completed', 0)
            total_failed += len(task.get('failed_files', []))
            
            # Check task status
            last_update = datetime.fromisoformat(task.get('iso_timestamp', current_time.isoformat()))
            time_since_update = current_time - last_update
            
            if time_since_update > timedelta(minutes=30):  # No update in 30 minutes
                stuck_tasks.append(task.get('batch_number', 'unknown'))
            else:
                active_tasks += 1
        
        # Estimate total files (assuming 550 per task for most tasks)
        estimated_total = len(progress_data) * 550  # Rough estimate
        
        return {
            'total_completed': total_completed,
            'total_failed': total_failed,
            'estimated_total': estimated_total,
            'completion_percentage': (total_completed / estimated_total * 100) if estimated_total > 0 else 0,
            'active_tasks': active_tasks,
            'completed_tasks': completed_tasks,
            'failed_tasks': failed_tasks,
            'stuck_tasks': stuck_tasks,
            'total_tasks': len(progress_data)
        }

    def display_progress(self, summary: Dict):
        """
        Displays formatted progress information
        """
        print("\n" + "="*60)
        print(f"CHIRPS Processing Progress - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        print(f"Files Completed: {summary['total_completed']:,}")
        print(f"Files Failed: {summary['total_failed']:,}")
        print(f"Estimated Total: {summary['estimated_total']:,}")
        print(f"Progress: {summary['completion_percentage']:.1f}%")
        print(f"Active Tasks: {summary['active_tasks']}")
        print(f"Total Tasks: {summary['total_tasks']}")
        
        if summary['stuck_tasks']:
            print(f"\nStuck Tasks (no update >30min): {', '.join(summary['stuck_tasks'])}")

    def monitor_continuously(self, interval_minutes: int = 5):
        """
        Monitors progress continuously with specified interval
        
        Parameters
        ----------
        interval_minutes : int
            Minutes between progress checks
        """
        print(f"Starting continuous monitoring (checking every {interval_minutes} minutes)")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                progress_data = self.get_all_progress_files()
                if progress_data:
                    summary = self.calculate_overall_progress(progress_data)
                    self.display_progress(summary)
                else:
                    print(f"No progress files found - {datetime.now().strftime('%H:%M:%S')}")
                
                time.sleep(interval_minutes * 60)
                
        except KeyboardInterrupt:
            print("\nMonitoring stopped")

    def get_failed_files_report(self) -> List[Dict]:
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

def main():
    monitor = ProgressMonitor()
    
    # Choose monitoring mode
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--continuous':
        monitor.monitor_continuously()
    elif len(sys.argv) > 1 and sys.argv[1] == '--failed':
        failed_files = monitor.get_failed_files_report()
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
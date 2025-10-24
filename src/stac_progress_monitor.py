"""
Progress monitor for STAC batch processing jobs.
"""

import json
import time
from datetime import datetime, timedelta
import os
from typing import Dict, List, Optional

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential


class StacProgressMonitor:
    """Monitor progress of STAC batch processing jobs."""
    
    def __init__(self, storage_account_url: str = None):
        """
        Initialize the progress monitor.
        
        Parameters:
            storage_account_url (str): Azure storage account URL
        """
        self.storage_account_url = storage_account_url or os.environ.get(
            "STORAGE_ACCOUNT_URL",
            "https://mpcpstorageaccount.blob.core.windows.net"
        )
        self.credential = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient(
            account_url=self.storage_account_url,
            credential=self.credential
        )
        self.container_name = "batch-logs"
    
    def get_stac_progress_files(self, job_prefix: str = "stac_") -> List[Dict]:
        """
        Retrieve all STAC task progress files from blob storage.
        
        Parameters:
            job_prefix (str): Prefix to filter STAC job files
            
        Returns:
            List[Dict]: List of progress data from all STAC tasks
        """
        progress_data = []
        
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blob_list = container_client.list_blobs()
            
            for blob in blob_list:
                if blob.name.startswith(job_prefix) and blob.name.endswith('_progress.json'):
                    try:
                        blob_client = container_client.get_blob_client(blob.name)
                        content = blob_client.download_blob().readall()
                        task_data = json.loads(content)
                        progress_data.append(task_data)
                        print(f"Loaded progress file: {blob.name}")
                    except Exception as e:
                        print(f"Error reading {blob.name}: {e}")
        
        except Exception as e:
            print(f"Error accessing container: {e}")
        
        return progress_data
    
    def calculate_overall_progress(self, progress_data: List[Dict]) -> Dict:
        """
        Aggregate progress data from all STAC processing tasks.
        
        Parameters:
            progress_data (List[Dict]): List of task progress data
            
        Returns:
            Dict: Overall progress summary
        """
        if not progress_data:
            return {
                'status': 'No progress data available',
                'total_completed': 0,
                'total_failed': 0,
                'total_items': 0
            }
        
        total_completed = sum(task.get('completed_count', 0) for task in progress_data)
        total_failed = sum(task.get('failed_count', 0) for task in progress_data)
        total_items = sum(task.get('total_items', 0) for task in progress_data)
        
        # Check for stuck tasks
        current_time = datetime.now()
        active_tasks = 0
        stuck_tasks = []
        completed_tasks = 0
        
        for task in progress_data:
            task_id = task.get('task_id', 'unknown')
            timestamp_str = task.get('timestamp', current_time.isoformat())
            
            try:
                last_update = datetime.fromisoformat(timestamp_str)
                time_since_update = current_time - last_update
                
                # Check if task is complete
                if task.get('completed_count', 0) + task.get('failed_count', 0) >= task.get('total_items', 0):
                    completed_tasks += 1
                elif time_since_update > timedelta(minutes=30):
                    stuck_tasks.append({
                        'task_id': task_id,
                        'last_update': timestamp_str,
                        'progress': task.get('progress_percentage', 0)
                    })
                else:
                    active_tasks += 1
            except Exception:
                pass
        
        overall_percentage = (total_completed / total_items * 100) if total_items > 0 else 0
        
        return {
            'total_completed': total_completed,
            'total_failed': total_failed,
            'total_items': total_items,
            'completion_percentage': overall_percentage,
            'active_tasks': active_tasks,
            'completed_tasks': completed_tasks,
            'stuck_tasks': stuck_tasks,
            'total_tasks': len(progress_data)
        }
    
    def display_progress(self, summary: Dict):
        """
        Display formatted progress information.
        
        Parameters:
            summary (Dict): Progress summary data
        """
        print("\n" + "="*70)
        print(f"STAC Processing Progress - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        print(f"📊 Overall Progress: {summary['completion_percentage']:.1f}%")
        print(f"✅ STAC Items Created: {summary['total_completed']:,}")
        print(f"❌ Failed Items: {summary['total_failed']:,}")
        print(f"📁 Total Items: {summary['total_items']:,}")
        print("-"*70)
        print(f"🔄 Active Tasks: {summary['active_tasks']}")
        print(f"✔️  Completed Tasks: {summary['completed_tasks']}")
        print(f"📋 Total Tasks: {summary['total_tasks']}")
        
        if summary['stuck_tasks']:
            print("\n⚠️  Stuck Tasks (no update >30min):")
            for stuck in summary['stuck_tasks']:
                print(f"  - {stuck['task_id']}: {stuck['progress']:.1f}% (last update: {stuck['last_update']})")
    
    def get_failed_items_report(self) -> List[Dict]:
        """
        Get detailed report of all failed STAC conversions.
        
        Returns:
            List[Dict]: List of failed items with details
        """
        progress_data = self.get_stac_progress_files()
        all_failed = []
        
        for task in progress_data:
            task_id = task.get('task_id', 'unknown')
            failed_items = task.get('failed_items', [])
            
            for item in failed_items:
                all_failed.append({
                    'task_id': task_id,
                    'blob_path': item.get('path'),
                    'error': item.get('error'),
                    'timestamp': task.get('timestamp')
                })
        
        return all_failed
    
    def monitor_continuously(self, interval_minutes: int = 5):
        """
        Monitor progress continuously with specified interval.
        
        Parameters:
            interval_minutes (int): Minutes between progress checks
        """
        print(f"Starting continuous monitoring (checking every {interval_minutes} minutes)")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                progress_data = self.get_stac_progress_files()
                if progress_data:
                    summary = self.calculate_overall_progress(progress_data)
                    self.display_progress(summary)
                    
                    # Check if all tasks are complete
                    if summary['active_tasks'] == 0 and summary['completed_tasks'] == summary['total_tasks']:
                        print("\n🎉 All tasks completed!")
                        break
                else:
                    print(f"No progress files found - {datetime.now().strftime('%H:%M:%S')}")
                
                time.sleep(interval_minutes * 60)
                
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")
    
    def generate_summary_report(self) -> Dict:
        """
        Generate a comprehensive summary report of the STAC processing job.
        
        Returns:
            Dict: Comprehensive report data
        """
        progress_data = self.get_stac_progress_files()
        summary = self.calculate_overall_progress(progress_data)
        failed_items = self.get_failed_items_report()
        
        report = {
            'report_timestamp': datetime.now().isoformat(),
            'summary': summary,
            'failed_items_count': len(failed_items),
            'failed_items_sample': failed_items[:10] if failed_items else [],
            'performance_metrics': {
                'average_progress': sum(t.get('progress_percentage', 0) for t in progress_data) / len(progress_data) if progress_data else 0,
                'tasks_with_failures': sum(1 for t in progress_data if t.get('failed_count', 0) > 0)
            }
        }
        
        return report
    
    def save_summary_report(self, output_path: str = "stac_processing_report.json"):
        """
        Save a summary report to a file.
        
        Parameters:
            output_path (str): Path to save the report
        """
        report = self.generate_summary_report()
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"Summary report saved to: {output_path}")
        return report


def main():
    """
    Main function to run the STAC progress monitor.
    """
    import sys
    import os
    
    monitor = StacProgressMonitor()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--continuous':
            # Continuous monitoring
            interval = int(sys.argv[2]) if len(sys.argv) > 2 else 5
            monitor.monitor_continuously(interval_minutes=interval)
            
        elif sys.argv[1] == '--failed':
            # Show failed items
            failed_items = monitor.get_failed_items_report()
            print(f"\nFound {len(failed_items)} failed STAC conversions:")
            for i, failure in enumerate(failed_items[:20], 1):
                print(f"{i}. Task {failure['task_id']}: {failure['blob_path']}")
                print(f"   Error: {failure['error'][:100]}...")
            
            if len(failed_items) > 20:
                print(f"\n... and {len(failed_items) - 20} more failures")
        
        elif sys.argv[1] == '--report':
            # Generate and save report
            report = monitor.save_summary_report()
            monitor.display_progress(report['summary'])
        
        else:
            print("Usage:")
            print("  python stac_progress_monitor.py                  # Single check")
            print("  python stac_progress_monitor.py --continuous [interval_minutes]")
            print("  python stac_progress_monitor.py --failed         # Show failed items")
            print("  python stac_progress_monitor.py --report         # Generate report")
    else:
        # Single check
        progress_data = monitor.get_stac_progress_files()
        if progress_data:
            summary = monitor.calculate_overall_progress(progress_data)
            monitor.display_progress(summary)
        else:
            print("No STAC progress files found")


if __name__ == "__main__":
    main()
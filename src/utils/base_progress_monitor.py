"""
Base progress monitor for batch processing jobs.
Consolidates shared functionality between STAC and CHIRPS progress monitors.
"""

import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Optional
from abc import ABC, abstractmethod

from src.utils.azure_storage_utils import get_blob_service_client


class BaseProgressMonitor(ABC):
    """
    Base class for monitoring batch processing job progress.

    Provides common functionality for retrieving, aggregating, and displaying
    progress data from Azure Blob Storage. Subclasses customize behavior through
    abstract methods and configuration.
    """

    def __init__(self, storage_account_url: str = None, container_name: str = "batch-logs"):
        """
        Initialize the progress monitor.

        Parameters:
            storage_account_url (str): Azure storage account URL (unused, kept for compatibility)
            container_name (str): Container name for progress files
        """
        self.blob_service_client = get_blob_service_client()
        self.container_name = container_name

    @abstractmethod
    def get_file_prefix(self) -> str:
        """Return the prefix for filtering progress files (e.g., 'stac_', 'task')."""
        pass

    @abstractmethod
    def get_job_name(self) -> str:
        """Return the job name for display (e.g., 'STAC Processing', 'CHIRPS Processing')."""
        pass

    @abstractmethod
    def extract_task_metrics(self, task: Dict) -> Dict:
        """
        Extract completed, failed, and total counts from a task progress dict.

        Returns:
            Dict with keys: completed, failed, total, task_id
        """
        pass

    def get_progress_files(self) -> List[Dict]:
        """
        Retrieve all progress files from blob storage.

        Returns:
            List[Dict]: List of progress data from all tasks
        """
        progress_data = []
        prefix = self.get_file_prefix()

        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blob_list = container_client.list_blobs()

            for blob in blob_list:
                # Filter by prefix and JSON extension
                if blob.name.startswith(prefix) and blob.name.endswith('.json'):
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
        Aggregate progress data from all tasks.

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
                'total_items': 0,
                'completion_percentage': 0,
                'active_tasks': 0,
                'completed_tasks': 0,
                'stuck_tasks': [],
                'total_tasks': 0
            }

        total_completed = 0
        total_failed = 0
        total_items = 0
        active_tasks = 0
        stuck_tasks = []
        completed_tasks = 0

        current_time = datetime.now()

        for task in progress_data:
            # Use abstract method to extract metrics
            metrics = self.extract_task_metrics(task)

            total_completed += metrics['completed']
            total_failed += metrics['failed']
            total_items += metrics['total']

            # Check task status based on last update time
            timestamp_str = task.get('timestamp') or task.get('iso_timestamp', current_time.isoformat())

            try:
                last_update = datetime.fromisoformat(timestamp_str)
                time_since_update = current_time - last_update

                # Check if task is complete
                if metrics['completed'] + metrics['failed'] >= metrics['total']:
                    completed_tasks += 1
                elif time_since_update > timedelta(minutes=30):
                    stuck_tasks.append({
                        'task_id': metrics['task_id'],
                        'last_update': timestamp_str,
                        'progress': (metrics['completed'] / metrics['total'] * 100) if metrics['total'] > 0 else 0
                    })
                else:
                    active_tasks += 1
            except Exception:
                pass

        completion_percentage = (total_completed / total_items * 100) if total_items > 0 else 0

        return {
            'total_completed': total_completed,
            'total_failed': total_failed,
            'total_items': total_items,
            'completion_percentage': completion_percentage,
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
        job_name = self.get_job_name()

        print("\n" + "="*70)
        print(f"{job_name} Progress - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)

        if summary.get('status'):
            print(summary['status'])
        else:
            print(f"📊 Overall Progress: {summary['completion_percentage']:.1f}%")
            print(f"✅ Completed: {summary['total_completed']:,}")
            print(f"❌ Failed: {summary['total_failed']:,}")
            print(f"📁 Total Items: {summary['total_items']:,}")
            print("-"*70)
            print(f"🔄 Active Tasks: {summary['active_tasks']}")
            print(f"✔️  Completed Tasks: {summary['completed_tasks']}")
            print(f"📋 Total Tasks: {summary['total_tasks']}")

            if summary['stuck_tasks']:
                print("\n⚠️  Stuck Tasks (no update >30min):")
                for stuck in summary['stuck_tasks']:
                    print(f"  - {stuck['task_id']}: {stuck['progress']:.1f}% (last update: {stuck['last_update']})")

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
                progress_data = self.get_progress_files()
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

    @abstractmethod
    def get_failed_items_report(self) -> List[Dict]:
        """Get detailed report of all failed items. Implementation varies by job type."""
        pass

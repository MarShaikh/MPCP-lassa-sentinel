"""
Progress monitor for STAC batch processing jobs.
"""

import json
import time
from datetime import datetime, timedelta
import os
from typing import Dict, List

from src.utils.base_progress_monitor import BaseProgressMonitor


class StacProgressMonitor(BaseProgressMonitor):
    """Monitor progress of STAC batch processing jobs."""

    def __init__(self, storage_account_url: str = None):
        """
        Initialize the progress monitor.

        Parameters:
            storage_account_url (str): Azure storage account URL (unused, kept for compatibility)
        """
        super().__init__(storage_account_url=storage_account_url, container_name="batch-logs")

    def get_file_prefix(self) -> str:
        """Return the prefix for filtering STAC progress files."""
        return "stac_"

    def get_job_name(self) -> str:
        """Return the job name for display."""
        return "STAC Processing"

    def extract_task_metrics(self, task: Dict) -> Dict:
        """
        Extract completed, failed, and total counts from a STAC task progress dict.

        Returns:
            Dict with keys: completed, failed, total, task_id
        """
        return {
            'completed': task.get('completed_count', 0),
            'failed': task.get('failed_count', 0),
            'total': task.get('total_items', 0),
            'task_id': task.get('task_id', 'unknown')
        }

    def get_stac_progress_files(self, job_prefix: str = "stac_") -> List[Dict]:
        """
        Retrieve all STAC task progress files from blob storage.

        Parameters:
            job_prefix (str): Prefix to filter STAC job files (kept for backward compatibility)

        Returns:
            List[Dict]: List of progress data from all STAC tasks
        """
        # Use the base class implementation
        return self.get_progress_files()
    
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
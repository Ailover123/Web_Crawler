"""
Centralized metrics tracking and terminal output formatting for the crawler.
Provides clean tabular output and comprehensive summaries.
"""

import time
import psutil
import os
from datetime import datetime, timedelta
from collections import defaultdict
from threading import Lock
from tabulate import tabulate
import sys


class CrawlerMetrics:
    """
    Centralized metrics tracker for crawler operations.
    Tracks per-URL and per-domain statistics with thread-safe operations.
    """
    
    def __init__(self):
        self.lock = Lock()
        self.start_time = time.time()
        
        # Per-URL tracking
        self.url_records = []
        self.url_counter = 0
        
        # Worker tracking
        self.worker_stats = defaultdict(lambda: {
            'urls_fetched': 0,
            'success_count': 0,
            'skipped_count': 0,
            'not_found_count': 0,
            'failed_count': 0,
            'cpu_samples': [],
            'memory_samples': [],
        })
        
        # Domain-based aggregation
        self.domain_stats = defaultdict(lambda: {
            'total_urls': 0,
            'success_count': 0,
            'skipped_count': 0,
            'not_found_count': 0,
            'failed_count': 0,
            'total_size_bytes': 0,
            'total_fetch_time': 0.0,
            'max_memory_mb': 0.0,
            'urls': [],
            'policy_stats': {}  # URL policy evaluation stats for this domain
        })
        
        # Overall metrics
        self.overall_stats = {
            'total_urls': 0,
            'success_count': 0,
            'skipped_count': 0,
            'not_found_count': 0,
            'failed_count': 0,
            'total_size_bytes': 0,
            'total_fetch_time': 0.0,
            'peak_memory_mb': 0.0,
            'start_time': datetime.now(),
        }
        
        # Resource tracking
        self.process = psutil.Process(os.getpid())
        self.initial_memory_mb = self.process.memory_info().rss / 1024 / 1024

        # DB writer failures (batch-level)
        self.db_batch_failures = []
        
    def record_url(self, url, domain, status, size_bytes, fetch_time_sec, 
                   memory_mb, worker_name, error_reason=None, is_retry=False):
        """
        Record metrics for a single URL crawl.
        
        Args:
            url: URL that was crawled
            domain: Domain of the URL
            status: 'success' or 'failure'
            size_bytes: Size of response in bytes
            fetch_time_sec: Time taken to fetch in seconds
            memory_mb: Memory consumption in MB
            worker_name: Name of worker thread
            error_reason: Reason for failure (if status='failure')
            is_retry: If True, update existing record instead of creating new one
        """
        with self.lock:
            # For retries, find and update existing record
            if is_retry:
                existing_record = next((r for r in self.url_records if r['url'] == url), None)
                if existing_record:
                    # Update existing record without changing serial number
                    old_status = existing_record['status']
                    existing_record['status'] = status
                    existing_record['size_bytes'] = size_bytes
                    existing_record['fetch_time_sec'] = fetch_time_sec
                    existing_record['memory_mb'] = memory_mb
                    existing_record['worker'] = worker_name
                    existing_record['error_reason'] = error_reason
                    existing_record['timestamp'] = datetime.now()
                    
                    # Update worker stats (decrement old, increment new)
                    ws = self.worker_stats[worker_name]
                    ws['urls_fetched'] += 1
                    if old_status == 'failed':
                        ws['failed_count'] -= 1
                    if status == 'success':
                        ws['success_count'] += 1
                    elif status == 'failed':
                        ws['failed_count'] += 1
                    
                    # Update domain stats
                    ds = self.domain_stats[domain]
                    if old_status == 'failed':
                        ds['failed_count'] -= 1
                    if status == 'success':
                        ds['success_count'] += 1
                    elif status == 'failed':
                        ds['failed_count'] += 1
                    ds['total_size_bytes'] += size_bytes
                    ds['total_fetch_time'] += fetch_time_sec
                    ds['max_memory_mb'] = max(ds['max_memory_mb'], memory_mb)
                    
                    # Update overall stats
                    if old_status == 'failed':
                        self.overall_stats['failed_count'] -= 1
                    if status == 'success':
                        self.overall_stats['success_count'] += 1
                    elif status == 'failed':
                        self.overall_stats['failed_count'] += 1
                    self.overall_stats['total_size_bytes'] += size_bytes
                    self.overall_stats['total_fetch_time'] += fetch_time_sec
                    self.overall_stats['peak_memory_mb'] = max(
                        self.overall_stats['peak_memory_mb'], memory_mb
                    )
                    return  # Exit early after updating
            
            # Not a retry or record not found - create new record
            self.url_counter += 1
            
            # Create URL record
            record = {
                'sr_no': self.url_counter,
                'url': url,
                'domain': domain,
                'status': status,
                'size_bytes': size_bytes,
                'fetch_time_sec': fetch_time_sec,
                'memory_mb': memory_mb,
                'worker': worker_name,
                'error_reason': error_reason,
                'timestamp': datetime.now()
            }
            
            self.url_records.append(record)
            
            # Update worker stats
            ws = self.worker_stats[worker_name]
            ws['urls_fetched'] += 1
            if status == 'success':
                ws['success_count'] += 1
            elif status == 'skipped':
                ws['skipped_count'] += 1
            elif status == 'not_found':
                ws['not_found_count'] += 1
            else:
                ws['failed_count'] += 1
            
            # Update domain stats
            ds = self.domain_stats[domain]
            ds['total_urls'] += 1
            ds['urls'].append(url)
            if status == 'success':
                ds['success_count'] += 1
            elif status == 'skipped':
                ds['skipped_count'] += 1
            elif status == 'not_found':
                ds['not_found_count'] += 1
            else:
                ds['failed_count'] += 1
            ds['total_size_bytes'] += size_bytes
            ds['total_fetch_time'] += fetch_time_sec
            ds['max_memory_mb'] = max(ds['max_memory_mb'], memory_mb)
            
            # Update overall stats
            self.overall_stats['total_urls'] += 1
            if status == 'success':
                self.overall_stats['success_count'] += 1
            elif status == 'skipped':
                self.overall_stats['skipped_count'] += 1
            elif status == 'not_found':
                self.overall_stats['not_found_count'] += 1
            else:
                self.overall_stats['failed_count'] += 1
            self.overall_stats['total_size_bytes'] += size_bytes
            self.overall_stats['total_fetch_time'] += fetch_time_sec
            self.overall_stats['peak_memory_mb'] = max(
                self.overall_stats['peak_memory_mb'], memory_mb
            )
    
            def record_db_batch_failure(self, reason: str, attempts: int, batch_size: int):
                """Track DB writer batch failures for end-of-crawl summary."""
                with self.lock:
                    self.db_batch_failures.append({
                        'reason': reason,
                        'attempts': attempts,
                        'batch_size': batch_size,
                        'timestamp': datetime.now(),
                    })
    
    def print_url_row(self, url, domain, status, size_bytes, fetch_time_sec, 
                      memory_mb, worker_name, error_reason=None):
        """
        Print a single URL crawl result in tabular format.
        """
        # Truncate URL for display
        display_url = url if len(url) <= 50 else url[:47] + "..."
        
        # Format size
        if size_bytes >= 1024 * 1024:
            size_str = f"{size_bytes / 1024 / 1024:.2f} MB"
        elif size_bytes >= 1024:
            size_str = f"{size_bytes / 1024:.2f} KB"
        else:
            size_str = f"{size_bytes} B"
        
        # Status with color (using ANSI codes)
        if status == 'success':
            status_str = f"\033[92m✓ SUCCESS\033[0m"  # Green
        elif status == 'skipped':
            status_str = f"\033[93m⏭ SKIPPED\033[0m"  # Yellow
        elif status == 'not_found':
            status_str = f"\033[94m🔍 NOT FOUND\033[0m"  # Blue
        else:
            status_str = f"\033[91m✗ FAILED\033[0m"  # Red
        
        # Create row data
        row = [
            self.url_counter,
            worker_name,
            display_url,
            domain,
            status_str,
            size_str,
            f"{fetch_time_sec:.3f}s",
            f"{memory_mb:.2f} MB"
        ]
        
        # Print with tabulate
        if self.url_counter == 1:
            # Print header on first row
            headers = ['#', 'Worker', 'URL', 'Domain', 'Status', 'Size', 'Time', 'Memory']
            print("\n" + "="*120)
            print("CRAWL PROGRESS")
            print("="*120)
            print(tabulate([row], headers=headers, tablefmt='simple'))
        else:
            print(tabulate([row], tablefmt='simple'))
        
        # Print error reason if any non-success status
        if status != 'success' and error_reason:
            print(f"  └─ Reason: {error_reason}")
    
    def get_current_memory_usage(self):
        """
        Get current memory usage of the process.
        Returns memory in MB and memory delta from start.
        """
        current_memory_mb = self.process.memory_info().rss / 1024 / 1024
        delta_mb = current_memory_mb - self.initial_memory_mb
        return current_memory_mb, delta_mb
    
    def collect_worker_stats(self, workers):
        """
        Collect CPU and memory stats from all worker threads.
        Called after crawl completes to get accurate measurements.
        """
        for worker in workers:
            if hasattr(worker, 'cpu_percent_samples') and hasattr(worker, 'memory_usage_samples'):
                self.worker_stats[worker.name]['cpu_samples'] = worker.cpu_percent_samples
                self.worker_stats[worker.name]['memory_samples'] = worker.memory_usage_samples
    
    def record_policy_stats_for_domain(self, domain, policy_stats):
        """
        Store URL policy evaluation stats for a specific domain.
        Called after each site crawl completes.
        
        Args:
            domain: Domain name
            policy_stats: Dict of policy stats from URLPolicy.get_stats()
        """
        with self.lock:
            if domain in self.domain_stats:
                self.domain_stats[domain]['policy_stats'] = policy_stats.copy()
    
    def print_progress_summary(self, frontier_stats, db_stats):
        """
        Print periodic progress summary during crawl.
        DISABLED: Progress updates removed from terminal output.
        Can be re-enabled for UI integration in the future.
        """
        pass
        # Commented out for cleaner terminal output
        # elapsed = time.time() - self.start_time
        # current_mem, delta_mem = self.get_current_memory_usage()
        # 
        # print("\n" + "-"*100)
        # print(f"⏱  PROGRESS UPDATE - Elapsed: {timedelta(seconds=int(elapsed))}")
        # print("-"*100)
        # 
        # # Overall progress
        # print(f"URLs: {self.overall_stats['total_urls']} total "
        #       f"({self.overall_stats['success_count']} success, "
        #       f"{self.overall_stats['failed_count']} failed)")
        # print(f"Queue: {frontier_stats['queue_size']} pending, "
        #       f"{frontier_stats['in_progress_count']} in progress")
        # print(f"Memory: {current_mem:.2f} MB (Δ {delta_mem:+.2f} MB from start)")
        # print(f"Storage: {db_stats['total_db_size_mb']:.2f} MB across all DBs")
        # 
        # # Per-domain breakdown
        # if self.domain_stats:
        #     print("\nPer-Domain Stats:")
        #     domain_rows = []
        #     for domain, stats in sorted(self.domain_stats.items()):
        #         domain_rows.append([
        #             domain,
        #             stats['total_urls'],
        #             stats['success_count'],
        #             stats['failed_count'],
        #             f"{stats['total_size_bytes'] / 1024 / 1024:.2f} MB",
        #             f"{db_stats['domain_stats'].get(domain, {}).get('db_size_mb', 0):.2f} MB"
        #         ])
        #     print(tabulate(domain_rows, 
        #                  headers=['Domain', 'URLs', 'Success', 'Failed', 'Data Size', 'DB Size'],
        #                  tablefmt='grid'))
        # print("-"*100 + "\n")
        #     print("\nPer-Domain Stats:")
        #     domain_rows = []
        #     for domain, stats in sorted(self.domain_stats.items()):
        #         domain_rows.append([
        #             domain,
        #             stats['total_urls'],
        #             stats['success_count'],
        #             stats['failed_count'],
        #             f"{stats['total_size_bytes'] / 1024 / 1024:.2f} MB",
        #             f"{db_stats['domain_stats'].get(domain, {}).get('db_size_mb', 0):.2f} MB"
        #         ])
        #     print(tabulate(domain_rows, 
        #                  headers=['Domain', 'URLs', 'Success', 'Failed', 'Data Size', 'DB Size'],
        #                  tablefmt='grid'))
        # print("-"*100 + "\n")
    
    def print_final_summary(self, db_stats, frontier_memory_stats):
        """
        Print comprehensive final summary after crawl completion.
        """
        elapsed = time.time() - self.start_time
        final_mem, delta_mem = self.get_current_memory_usage()
        
        print("\n" + "="*100)
        print("CRAWL COMPLETED - FINAL SUMMARY")
        print("="*100)
        
        # Time metrics
        print(f"\nTIME METRICS:")
        print(f"   Start Time:      {self.overall_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   End Time:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Total Duration:  {timedelta(seconds=int(elapsed))}")
        print(f"   Avg per URL:     {elapsed / max(1, self.overall_stats['total_urls']):.3f}s")
        
        # URL metrics
        print(f"\nURL METRICS:")
        print(f"   Total URLs Processed:  {self.overall_stats['total_urls']}")
        print(f"   Successfully Crawled: {self.overall_stats['success_count']} "
              f"({self.overall_stats['success_count'] / max(1, self.overall_stats['total_urls']) * 100:.1f}%)")
        print(f"   Skipped (Assets):     {self.overall_stats['skipped_count']} "
              f"({self.overall_stats['skipped_count'] / max(1, self.overall_stats['total_urls']) * 100:.1f}%)")
        print(f"   Not Found (404):     {self.overall_stats['not_found_count']} "
              f"({self.overall_stats['not_found_count'] / max(1, self.overall_stats['total_urls']) * 100:.1f}%)")
        print(f"   Failed (Errors):      {self.overall_stats['failed_count']} "
              f"({self.overall_stats['failed_count'] / max(1, self.overall_stats['total_urls']) * 100:.1f}%)")
        
        # Data transfer metrics
        print(f"\nDATA TRANSFER:")
        total_mb = self.overall_stats['total_size_bytes'] / 1024 / 1024
        print(f"   Total Data Fetched:    {total_mb:.2f} MB ({self.overall_stats['total_size_bytes']:,} bytes)")
        print(f"   Avg per URL:           {self.overall_stats['total_size_bytes'] / max(1, self.overall_stats['total_urls']) / 1024:.2f} KB")
        print(f"   Throughput:            {total_mb / max(0.001, elapsed):.2f} MB/s")

        # DB writer failures (if any)
        if self.db_batch_failures:
            print(f"\nDB WRITER FAILURES: {len(self.db_batch_failures)} batch(es) failed after retries")
            for idx, fail in enumerate(self.db_batch_failures, start=1):
                ts = fail['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                print(f"   {idx}. {ts} | attempts={fail['attempts']} | batch_size={fail['batch_size']} | reason={fail['reason']}")
        
        # Memory metrics (for scaling)
        print(f"\nMEMORY CONSUMPTION (for scaling):")
        print(f"   Initial Memory:        {self.initial_memory_mb:.2f} MB")
        print(f"   Final Memory:          {final_mem:.2f} MB")
        print(f"   Peak Memory:           {self.overall_stats['peak_memory_mb']:.2f} MB")
        print(f"   Memory Delta:          {delta_mem:+.2f} MB")
        print(f"   Memory per URL:        {delta_mem / max(1, self.overall_stats['total_urls']):.4f} MB/URL")
        print(f"   Frontier Memory:       {frontier_memory_stats['frontier_memory_mb']:.2f} MB")
        print(f"     - Queue:             {frontier_memory_stats['queue_memory_mb']:.2f} MB")
        print(f"     - Visited Set:       {frontier_memory_stats['visited_memory_mb']:.2f} MB")
        print(f"     - In-Progress Set:   {frontier_memory_stats['in_progress_memory_mb']:.2f} MB")
        print(f"     - Discovered Set:    {frontier_memory_stats['discovered_memory_mb']:.2f} MB")
        print(f"     - Media Assets:      {frontier_memory_stats['assets_memory_mb']:.2f} MB ({frontier_memory_stats['total_unique_assets']} unique media files)")
        print(f"       (PDFs, images, docs - deduplicated across all pages, CSS/JS excluded)")
        
        # Storage metrics
        print(f"\nSTORAGE:")
        print(f"   Total DB Size:         {db_stats['total_db_size_mb']:.2f} MB")
        print(f"   Total DB Rows:         {db_stats['total_rows']:,}")
        print(f"   Storage per URL:       {db_stats['total_db_size_mb'] / max(1, self.overall_stats['total_urls']):.4f} MB/URL")
        
        # Performance metrics
        print(f"\nPERFORMANCE:")
        print(f"   Total Fetch Time:      {timedelta(seconds=int(self.overall_stats['total_fetch_time']))}")
        print(f"   Avg Fetch Time/URL:    {self.overall_stats['total_fetch_time'] / max(1, self.overall_stats['total_urls']):.3f}s")
        print(f"   Crawl Speed:           {self.overall_stats['total_urls'] / max(0.001, elapsed):.2f} URLs/s")
        
        # Worker statistics with CPU and Memory tracking
        print(f"\nWORKER STATISTICS:")
        print(f"   Total Workers Used:    {len(self.worker_stats)}")
        worker_rows = []
        total_worker_cpu = 0
        total_worker_memory = 0
        worker_count_with_data = 0
        
        for worker_name in sorted(self.worker_stats.keys()):
            ws = self.worker_stats[worker_name]
            success_rate = f"{ws['success_count'] / max(1, ws['urls_fetched']) * 100:.1f}%"
            
            # Calculate averages
            avg_cpu = sum(ws['cpu_samples']) / len(ws['cpu_samples']) if ws['cpu_samples'] else 0
            avg_memory = sum(ws['memory_samples']) / len(ws['memory_samples']) if ws['memory_samples'] else 0
            
            if ws['cpu_samples']:
                total_worker_cpu += avg_cpu
                total_worker_memory += avg_memory
                worker_count_with_data += 1
            
            worker_rows.append([
                worker_name,
                ws['urls_fetched'],
                ws['success_count'],
                ws['skipped_count'],
                ws['not_found_count'],
                ws['failed_count'],
                success_rate,
                f"{avg_cpu:.1f}%",
                f"{avg_memory:.2f}MB"
            ])
        
        if worker_rows:
            print(tabulate(worker_rows,
                          headers=['Worker', 'URLs', 'Success', 'Skipped', '404s', 'Failed', 'Success %', 'Avg CPU', 'Avg Memory'],
                          tablefmt='grid'))
        
        # Worker resource summary
        if worker_count_with_data > 0:
            print(f"\nWORKER RESOURCE SUMMARY:")
            avg_cpu_per_worker = total_worker_cpu / worker_count_with_data
            cores_used = (len(self.worker_stats) * avg_cpu_per_worker) / 100
            
            print(f"   Avg CPU per Worker:    {avg_cpu_per_worker:.1f}%")
            print(f"   Avg Memory per Worker: {total_worker_memory / worker_count_with_data:.2f} MB")
            print(f"   Total Worker Memory:   {total_worker_memory:.2f} MB (all workers combined)")
            print(f"\n   CPU CORE BREAKDOWN:")
            print(f"   Cores Used:            {cores_used:.2f} out of 6 physical cores")
            print(f"   Per Worker:            1 worker ≈ 1 core at {avg_cpu_per_worker:.1f}% utilization")
            print(f"   Utilization:           {(cores_used/6)*100:.1f}% of system capacity")
        
        # Domain-based distribution
        if self.domain_stats:
            print(f"\nDOMAIN-BASED DISTRIBUTION:")
            domain_rows = []
            for domain in sorted(self.domain_stats.keys()):
                stats = self.domain_stats[domain]
                domain_rows.append([
                    domain,
                    stats['total_urls'],
                    stats['success_count'],
                    stats['skipped_count'],
                    stats['not_found_count'],
                    stats['failed_count'],
                    f"{stats['success_count'] / max(1, stats['total_urls']) * 100:.1f}%"
                ])
            if domain_rows:
                print(tabulate(domain_rows,
                              headers=['Domain', 'Total', 'Success', 'Skipped', '404s', 'Failed', 'Success %'],
                              tablefmt='grid'))
        
        # Per-domain breakdown - DISABLED for cleaner output
        # Can be re-enabled for UI integration in the future
        # print(f"\n🌍 PER-DOMAIN BREAKDOWN:")
        # domain_rows = []
        # for domain in sorted(self.domain_stats.keys()):
        #     stats = self.domain_stats[domain]
        #     db_info = db_stats['domain_stats'].get(domain, {})
        #     domain_rows.append([
        #         domain,
        #         stats['total_urls'],
        #         stats['success_count'],
        #         stats['failed_count'],
        #         f"{stats['success_count'] / max(1, stats['total_urls']) * 100:.1f}%",
        #         f"{stats['total_size_bytes'] / 1024 / 1024:.2f} MB",
        #         f"{db_info.get('db_size_mb', 0):.2f} MB",
        #         f"{stats['max_memory_mb']:.2f} MB"
        #     ])
        # 
        # print(tabulate(domain_rows, 
        #               headers=['Domain', 'Total', 'Success', 'Failed', 'Success %', 
        #                       'Data Size', 'DB Size', 'Peak Mem'],
        #               tablefmt='grid'))
        
        # Scaling recommendations
        print(f"\nSCALING RECOMMENDATIONS:")
        urls_per_mb = self.overall_stats['total_urls'] / max(0.1, delta_mem)
        print(f"   Memory Efficiency:     {urls_per_mb:.2f} URLs per MB")
        print(f"   For 10,000 URLs:       ~{10000 / max(1, urls_per_mb):.0f} MB memory needed")
        print(f"   For 100,000 URLs:      ~{100000 / max(1, urls_per_mb):.0f} MB memory needed")
        print(f"   For 1,000,000 URLs:    ~{1000000 / max(1, urls_per_mb) / 1024:.1f} GB memory needed")
        
        storage_per_url = db_stats['total_db_size_mb'] / max(1, self.overall_stats['total_urls'])
        print(f"   Storage Efficiency:    {storage_per_url:.4f} MB per URL")
        print(f"   For 10,000 URLs:       ~{10000 * storage_per_url:.0f} MB storage needed")
        print(f"   For 100,000 URLs:      ~{100000 * storage_per_url / 1024:.1f} GB storage needed")
        print(f"   For 1,000,000 URLs:    ~{1000000 * storage_per_url / 1024:.1f} GB storage needed")
        
        # Failed URLs detailed breakdown
        # Failed URLs breakdown by category
        skipped_records = [r for r in self.url_records if r['status'] == 'skipped']
        not_found_records = [r for r in self.url_records if r['status'] == 'not_found']
        failed_records = [r for r in self.url_records if r['status'] == 'failed']
        
        if skipped_records:
            print(f"\nSKIPPED URLs ({len(skipped_records)} total - Assets/Media):")
            skipped_rows = []
            for record in skipped_records:
                skipped_rows.append([
                    record['url'][:80] + '...' if len(record['url']) > 80 else record['url'],
                    record['error_reason'] or 'Unknown',
                    record['worker']
                ])
            print(tabulate(skipped_rows,
                          headers=['URL', 'Content Type', 'Worker'],
                          tablefmt='grid',
                          maxcolwidths=[80, 30, 15]))
        
        if not_found_records:
            print(f"\nNOT FOUND URLs ({len(not_found_records)} total - 404 Errors):")
            not_found_rows = []
            for record in not_found_records:
                not_found_rows.append([
                    record['url'][:80] + '...' if len(record['url']) > 80 else record['url'],
                    record['worker']
                ])
            print(tabulate(not_found_rows,
                          headers=['URL', 'Worker'],
                          tablefmt='simple_grid',
                          maxcolwidths=[80, 15]))
        
        if failed_records:
            print(f"\nFAILED URLs ({len(failed_records)} total - Timeouts/Errors):")
            failed_rows = []
            for record in failed_records:
                failed_rows.append([
                    record['url'][:80] + '...' if len(record['url']) > 80 else record['url'],
                    record['error_reason'] or 'Unknown',
                    record['worker']
                ])
            print(tabulate(failed_rows,
                          headers=['URL', 'Reason', 'Worker'],
                          tablefmt='grid',
                          maxcolwidths=[80, 30, 15]))
        
        print("\n" + "="*100)
        import sys
        sys.stdout.flush()

    def write_domain_stats_to_json(self, output_file=None):
        """
        Write per-domain statistics to a JSON file in the data folder.
        """
        import json
        from pathlib import Path
        
        if output_file is None:
            # Default to data folder with timestamp
            data_dir = Path(__file__).resolve().parents[2] / 'data'
            data_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = data_dir / f"domain_stats_{timestamp}.json"
        else:
            output_file = Path(output_file)
            output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Compile domain stats
        domain_data = {}
        total_domains = 0
        totals_accumulator = {
            'total_urls': 0,
            'success_count': 0,
            'skipped_count': 0,
            'not_found_count': 0,
            'failed_count': 0,
            'total_size_bytes': 0,
            'total_crawl_time_seconds': 0.0,
        }
        with self.lock:
            for domain in sorted(self.domain_stats.keys()):
                stats = self.domain_stats[domain]
                total_urls = stats['total_urls']
                total_crawl_time = stats['total_fetch_time']
                domain_entry = {
                    'total_urls': total_urls,
                    'success_count': stats['success_count'],
                    'skipped_count': stats['skipped_count'],
                    'not_found_count': stats['not_found_count'],
                    'failed_count': stats['failed_count'],
                    'total_size_bytes': stats['total_size_bytes'],
                    'total_size_mb': stats['total_size_bytes'] / 1024 / 1024,
                    'total_size_gb': stats['total_size_bytes'] / 1_000_000_000,
                    'total_crawl_time_seconds': total_crawl_time,
                    'total_crawl_time_minutes': total_crawl_time / 60.0,
                    'avg_crawl_time_seconds': total_crawl_time / max(1, total_urls),
                    'avg_crawl_time_minutes': (total_crawl_time / max(1, total_urls)) / 60.0,
                    'max_memory_mb': stats['max_memory_mb'],
                    'success_rate_percent': (stats['success_count'] / max(1, total_urls)) * 100,
                }
                
                # Add policy stats if available
                if stats['policy_stats']:
                    domain_entry['policy_stats'] = stats['policy_stats']
                
                domain_data[domain] = domain_entry
                # Update totals
                total_domains += 1
                totals_accumulator['total_urls'] += total_urls
                totals_accumulator['success_count'] += stats['success_count']
                totals_accumulator['skipped_count'] += stats['skipped_count']
                totals_accumulator['not_found_count'] += stats['not_found_count']
                totals_accumulator['failed_count'] += stats['failed_count']
                totals_accumulator['total_size_bytes'] += stats['total_size_bytes']
                totals_accumulator['total_crawl_time_seconds'] += total_crawl_time
        
        # Append overall summary in GB and minutes
        summary = {
            'total_domains': total_domains,
            'total_urls': totals_accumulator['total_urls'],
            'success_count': totals_accumulator['success_count'],
            'skipped_count': totals_accumulator['skipped_count'],
            'not_found_count': totals_accumulator['not_found_count'],
            'failed_count': totals_accumulator['failed_count'],
            'total_size_bytes': totals_accumulator['total_size_bytes'],
            'total_size_mb': totals_accumulator['total_size_bytes'] / 1024 / 1024,
            'total_size_gb': totals_accumulator['total_size_bytes'] / 1_000_000_000,
            'total_crawl_time_seconds': totals_accumulator['total_crawl_time_seconds'],
            'total_crawl_time_minutes': totals_accumulator['total_crawl_time_seconds'] / 60.0,
        }

        # Write to JSON file
        try:
            with open(output_file, 'w') as f:
                # Write domain data with trailing summary block
                domain_data_with_summary = dict(domain_data)
                domain_data_with_summary['summary'] = summary
                json.dump(domain_data_with_summary, f, indent=2)
            print(f"\n✓ Domain statistics written to: {output_file}")
            return str(output_file)
        except Exception as e:
            print(f"\n✗ Failed to write domain stats to JSON: {e}")
            return None


# Global metrics instance
_metrics = None

def get_metrics():
    """Get or create global metrics instance."""
    global _metrics
    if _metrics is None:
        _metrics = CrawlerMetrics()
    return _metrics

def reset_metrics():
    """Reset global metrics instance."""
    global _metrics
    _metrics = CrawlerMetrics()
    return _metrics

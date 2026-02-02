#!/usr/bin/env python3
"""
Script to run main.py and log all terminal output to a .txt file in real-time.
"""

import subprocess
import sys
import os
import time
import datetime
import re

def main():
    # Ensure we're in the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if os.getcwd() != script_dir:
        os.chdir(script_dir)

    # Create logs directory if it doesn't exist
    log_dir = os.path.join(script_dir, '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Generate timestamped log filename
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_filename = f"{timestamp}_IST.txt"
    log_path = os.path.join(log_dir, log_filename)
    
    print(f"Logging output to: {log_path}")

    # Run main.py and capture all output in real-time
    baseline_stats = {
        "created": None,
        "updated": None,
        "failed": None,
    }

    stat_patterns = {
        "created": re.compile(r"Baselines Created\s*:\s*(\d+)", re.IGNORECASE),
        "updated": re.compile(r"Baselines Updated\s*:\s*(\d+)", re.IGNORECASE),
        "failed": re.compile(r"Baselines Failed\s*:\s*(\d+)", re.IGNORECASE),
    }

    # DISABLED per user request: with open(log_path, 'w', encoding='utf-8') as f:
    # Use a more robust way to read output without crashing on character encoding issues
    process = subprocess.Popen([sys.executable, 'main.py'] + sys.argv[1:], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while True:
        line_bytes = process.stdout.readline()
        if not line_bytes and process.poll() is not None:
            break
        if line_bytes:
            # Decode with 'replace' to handle non-UTF-8 bytes (like 0xbf) gracefully
            output = line_bytes.decode('utf-8', errors='replace')
            print(output.strip())  # Print to terminal

            # Capture baseline summaries when they show up in BASELINE mode
            for key, pattern in stat_patterns.items():
                match = pattern.search(output)
                if match:
                    baseline_stats[key] = int(match.group(1))

    rc = process.poll()

    # Append baseline summary if we captured any stats
    if any(value is not None for value in baseline_stats.values()):
        summary = (
            "\n[BASELINE SUMMARY] "
            f"Created={baseline_stats['created'] if baseline_stats['created'] is not None else 'N/A'}, "
            f"Updated={baseline_stats['updated'] if baseline_stats['updated'] is not None else 'N/A'}, "
            f"Failed={baseline_stats['failed'] if baseline_stats['failed'] is not None else 'N/A'}\n"
        )
        print(summary.strip())

    print(f"\nCrawl completed.")

if __name__ == "__main__":
    main()

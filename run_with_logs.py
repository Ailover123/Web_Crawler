#!/usr/bin/env python3
"""
Crawler runner with real-time log capture
Captures all crawler output to a timestamped log file in logs/ folder
Usage: python run_with_logs.py --mode baseline
"""

import sys
import os
import subprocess
import datetime
import argparse
from pathlib import Path


def get_ist_timestamp():
    """Get current timestamp in IST (India Standard Time)"""
    utc_now = datetime.datetime.utcnow()
    ist_offset = datetime.timedelta(hours=5, minutes=30)
    ist_now = utc_now + ist_offset
    return ist_now


def create_log_filename():
    """Create log filename with date and IST time"""
    ist_now = get_ist_timestamp()
    # Format: YYYY-MM-DD_HH-MM-SS_IST.txt
    filename = ist_now.strftime("%Y-%m-%d_%H-%M-%S_IST.txt")
    return filename


def run_crawler_with_logs(mode):
    """Run crawler and capture logs"""
    
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Create log file
    log_filename = create_log_filename()
    log_path = logs_dir / log_filename
    
    print(f"Starting crawler in {mode.upper()} mode...")
    print(f"Log file: {log_path}")
    print("=" * 60)
    
    # Build command
    cmd = [
        sys.executable,
        "baseline-crawler/main.py"
    ]
    
    # Set environment variable
    env = os.environ.copy()
    env["CRAWL_MODE"] = mode.upper()
    
    try:
        with open(log_path, "w") as log_file:
            # Start crawler process
            process = subprocess.Popen(
                cmd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1  # Line-buffered
            )
            
            # Print and log output in real-time
            for line in process.stdout:
                line = line.rstrip()
                print(line)
                log_file.write(line + "\n")
                log_file.flush()  # Ensure line is written immediately
            
            # Wait for process to complete
            process.wait()
            
            # Write exit status to log
            exit_status = f"\n\n{'='*60}\nProcess exited with status: {process.returncode}\n{'='*60}"
            print(exit_status)
            log_file.write(exit_status)
            
            if process.returncode == 0:
                print(f"\n✓ Crawl completed successfully!")
                print(f"Log saved to: {log_path}")
            else:
                print(f"\n✗ Crawl failed with exit code {process.returncode}")
                print(f"Log saved to: {log_path}")
            
            return process.returncode
    
    except KeyboardInterrupt:
        print("\n\nCrawl interrupted by user")
        log_file.write("\n\nCrawl interrupted by user\n")
        return 1
    
    except Exception as e:
        print(f"Error running crawler: {e}")
        return 1


def main():
    parser = argparse.ArgumentParser(
        description="Run web crawler with real-time log capture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_with_logs.py --mode baseline
  python run_with_logs.py --mode compare
  python run_with_logs.py --mode crawl
        """
    )
    
    parser.add_argument(
        "--mode",
        required=True,
        choices=["baseline", "compare", "crawl"],
        help="Crawl mode: baseline (create snapshots), compare (detect changes), crawl (standard crawl)"
    )
    
    args = parser.parse_args()
    
    exit_code = run_crawler_with_logs(args.mode)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

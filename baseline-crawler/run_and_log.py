#!/usr/bin/env python3
"""
Script to run main.py and log all terminal output to a .txt file in real-time.
"""

import subprocess
import sys
import os
import time
import datetime

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
    with open(log_path, 'w', encoding='utf-8') as f:
        # Use a more robust way to read output without crashing on character encoding issues
        process = subprocess.Popen([sys.executable, 'main.py'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while True:
            line_bytes = process.stdout.readline()
            if not line_bytes and process.poll() is not None:
                break
            if line_bytes:
                # Decode with 'replace' to handle non-UTF-8 bytes (like 0xbf) gracefully
                output = line_bytes.decode('utf-8', errors='replace')
                print(output.strip())  # Print to terminal
                f.write(output)  # Write to file
                f.flush()  # Ensure it's written immediately
        rc = process.poll()

    print(f"\nCrawl completed. Output saved to {log_path}")

if __name__ == "__main__":
    main()

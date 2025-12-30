#!/usr/bin/env python3
"""
Script to run main.py and log all terminal output to a .txt file in real-time.
"""

import subprocess
import sys
import os
import time

def main():
    # Ensure we're in the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if os.getcwd() != script_dir:
        os.chdir(script_dir)

    # Run main.py and capture all output in real-time
    with open('crawl_output.txt', 'w') as f:
        process = subprocess.Popen([sys.executable, 'main.py'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(output.strip())  # Print to terminal
                f.write(output)  # Write to file
                f.flush()  # Ensure it's written immediately
        rc = process.poll()

    print("\nCrawl completed. Output saved to crawl_output.txt")

if __name__ == "__main__":
    main()

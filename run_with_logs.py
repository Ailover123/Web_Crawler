import subprocess
import sys
import os
from datetime import datetime

def run_crawler():
    # 1. Prepare argument forwarding
    # Support default --mode=detection if no args provided
    args = sys.argv[1:] if len(sys.argv) > 1 else ["--mode=detection"]
    # Force unbuffered output so logs appear immediately
    cmd = [sys.executable, "-u", "main.py"] + args

    # 2. Create logs directory
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # 3. Prepare timestamped log file (Machine-sortable)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"logs/crawl_session_{timestamp}.txt"
    
    print(f"--- STARTING CRAWLER WRAPPER ---")
    print(f"Log File: {log_filename}")
    print(f"Command:  {' '.join(cmd)}")
    print(f"--------------------------------\n")

    # 4. Execute main.py and capture output in real-time
    full_output = []
    
    # We use bufsize=1 for line-buffered output to stream live
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )

    with open(log_filename, "w", encoding="utf-8") as f:
        # Header for the log file
        f.write(f"--- Forensics Log: {timestamp} ---\n")
        f.write(f"--- Command: {' '.join(cmd)} ---\n\n")
        
        # Stream from process stdout (which includes stderr due to STDOUT redirect)
        for line in process.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            f.write(line)
            f.flush() # Ensure file is written immediately
            full_output.append(line)

    process.wait()
    
    # 5. Metadata and Verification
    summary_found = any("CRAWL SESSION SUMMARY" in line for line in full_output)
    
    with open(log_filename, "a", encoding="utf-8") as f:
        f.write(f"\n--- PROCESS EXIT CODE: {process.returncode} ---\n")
        if not summary_found:
            f.write("--- SUMMARY NOT FOUND: SESSION INVALID ---\n")
    
    if not summary_found:
        print("\n" + "!" * 40)
        print("ERROR: Crawl finished without terminal summary.")
        print("This violates reporting invariants.")
        print("!" * 40)
        sys.exit(1)
    
    if process.returncode != 0:
        print(f"\nCrawler exited with non-zero status code: {process.returncode}")
        sys.exit(process.returncode)

    print(f"\n--- WRAPPER COMPLETED SUCCESSFULLY ---")

if __name__ == "__main__":
    run_crawler()

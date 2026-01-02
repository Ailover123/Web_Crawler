#!/usr/bin/env python3
"""
Analysis generator script.
Scans data/ for DBs, generates JSONs with stats/classifications from each DB.
"""

import os
import json
import sqlite3
from pathlib import Path
from crawler.config import DATA_DIR
from crawler.storage.db import get_connection
from crawler.parser import classify_url

def generate_analysis_for_domain(domain, db_path):
    """
    Generate analysis JSON for a specific domain from its DB.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()

    # Get all URLs from crawl_data table
    cursor.execute("SELECT url FROM crawl_data")
    urls = [row[0] for row in cursor.fetchall()]

    conn.close()

    # Classify URLs
    distribution = {}
    for url in urls:
        url_type = classify_url(url)
        if isinstance(url_type, (set, list, tuple)):
            types_iter = list(url_type) if url_type else ["unknown"]
        else:
            types_iter = [url_type]

        for t in types_iter:
            if t not in distribution:
                distribution[t] = {"count": 0, "urls": []}
            distribution[t]["count"] += 1
            distribution[t]["urls"].append({"sr": len(distribution[t]["urls"]) + 1, "url": url})

    domain_analysis = {
        "domain": domain,
        "total_urls": len(urls),
        "distribution": distribution
    }

    return domain_analysis

def main():
    """
    Scan data/ for DBs and generate analysis JSONs.
    """
    data_dir = Path(DATA_DIR)
    if not data_dir.exists():
        print(f"Data directory {data_dir} does not exist.")
        return

    # Find all .db files (exclude old_runs)
    db_files = list(data_dir.glob("*.db"))
    if not db_files:
        print("No DB files found in data/.")
        return

    for db_file in db_files:
        domain = db_file.stem.replace("data_", "")
        print(f"Generating analysis for {domain}...")

        try:
            analysis = generate_analysis_for_domain(domain, db_file)
            json_file = data_dir / f"{domain}_analysis.json"
            with open(json_file, 'w') as f:
                json.dump(analysis, f, indent=4)
            print(f"Saved analysis to {json_file}")
        except Exception as e:
            print(f"Error generating analysis for {domain}: {e}")

if __name__ == "__main__":
    main()

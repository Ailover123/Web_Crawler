#!/usr/bin/env python3
"""
Check DB contents for debugging comparison issues.
"""

import sqlite3
from pathlib import Path

data_dir = Path('data')
current_db = data_dir / 'data_worldpeoplesolutions.com.db'
old_runs_dir = data_dir / 'old_runs'

print('Current DB:')
if current_db.exists():
    conn = sqlite3.connect(current_db)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM crawl_data')
    current_count = cursor.fetchone()[0]
    print(f'  URLs: {current_count}')

    # Get first few URLs
    cursor.execute('SELECT url FROM crawl_data LIMIT 5')
    urls = cursor.fetchall()
    print('  Sample URLs:')
    for url in urls:
        print(f'    {url[0]}')

    conn.close()
else:
    print('  Not found')

print('\nOld runs:')
if old_runs_dir.exists():
    for db_file in old_runs_dir.glob('*.db'):
        print(f'  {db_file.name}:')
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM crawl_data')
        count = cursor.fetchone()[0]
        print(f'    URLs: {count}')

        # Get first few URLs
        cursor.execute('SELECT url FROM crawl_data LIMIT 5')
        urls = cursor.fetchall()
        print('    Sample URLs:')
        for url in urls:
            print(f'      {url[0]}')

        conn.close()
else:
    print('  old_runs directory not found')

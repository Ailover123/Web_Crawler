import sqlite3
import os
import sys

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawler.config import DATA_DIR

def check_alerts():
    conn = sqlite3.connect('data/crawler.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all alerts
    cursor.execute('SELECT * FROM diff_evidence WHERE status="open" ORDER BY detected_at DESC')
    alerts = cursor.fetchall()

    print('=== ALERTS IN DATABASE ===')
    for alert in alerts:
        print(f'ID: {alert["id"]}, URL: {alert["url"]}')
        print(f'Baseline Hash: {alert["baseline_hash"]}')
        print(f'Observed Hash: {alert["observed_hash"]}')
        print(f'Severity: {alert["severity"]}')

        # Check if files exist
        baseline_path = os.path.join(DATA_DIR, 'snapshots', 'baselines', f'baseline_{alert["baseline_hash"]}.html')
        observed_path = os.path.join(DATA_DIR, 'snapshots', 'observed', f'observed_{alert["observed_hash"]}.html')

        print(f'Baseline file exists: {os.path.exists(baseline_path)}')
        print(f'Observed file exists: {os.path.exists(observed_path)}')

        if os.path.exists(baseline_path):
            with open(baseline_path, 'r', encoding='utf-8', errors='ignore') as f:
                baseline_content = f.read()
                print(f'Baseline file size: {len(baseline_content)} characters')
        else:
            print('Baseline file: NOT FOUND')

        if os.path.exists(observed_path):
            with open(observed_path, 'r', encoding='utf-8', errors='ignore') as f:
                observed_content = f.read()
                print(f'Observed file size: {len(observed_content)} characters')
        else:
            print('Observed file: NOT FOUND')

        print('---')

    conn.close()

if __name__ == '__main__':
    check_alerts()

import sqlite3
import os
import sys

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def debug_alerts():
    conn = sqlite3.connect('data/crawler.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Check total alerts and severity counts
    cursor.execute('SELECT COUNT(*) as total_alerts FROM diff_evidence WHERE status="open"')
    total_alerts = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) as high_severity FROM diff_evidence WHERE severity="high" AND status="open"')
    high_count = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) as medium_severity FROM diff_evidence WHERE severity="medium" AND status="open"')
    medium_count = cursor.fetchone()[0]

    print(f'Total open alerts: {total_alerts}')
    print(f'High severity alerts: {high_count}')
    print(f'Medium severity alerts: {medium_count}')

    # Check a few recent alerts to see their severity
    cursor.execute('SELECT id, url, severity, detected_at, baseline_hash, observed_hash FROM diff_evidence WHERE status="open" ORDER BY detected_at DESC LIMIT 10')
    recent_alerts = cursor.fetchall()

    print('\n=== RECENT ALERTS ===')
    for alert in recent_alerts:
        print(f'ID: {alert["id"]}, URL: {alert["url"]}, Severity: {alert["severity"]}')
        print(f'  Baseline Hash: {alert["baseline_hash"]}')
        print(f'  Observed Hash: {alert["observed_hash"]}')
        print(f'  Time: {alert["detected_at"]}')

    # Check if there are any alerts with severity 'HIGH' (uppercase)
    cursor.execute('SELECT COUNT(*) as high_upper FROM diff_evidence WHERE severity="HIGH" AND status="open"')
    high_upper = cursor.fetchone()[0]
    print(f'\nHigh severity (uppercase): {high_upper}')

    # Check all unique severity values
    cursor.execute('SELECT DISTINCT severity FROM diff_evidence WHERE status="open"')
    severities = cursor.fetchall()
    print(f'\nUnique severity values: {[s["severity"] for s in severities]}')

    conn.close()

if __name__ == '__main__':
    debug_alerts()

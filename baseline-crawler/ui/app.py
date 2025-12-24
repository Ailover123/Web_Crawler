from flask import Flask, render_template, redirect, url_for
import sqlite3
import json
import os
from datetime import datetime, timezone, timedelta
import sys
import difflib
from pathlib import Path

# Add parent directory to Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = Flask(__name__)

# Database path relative to the ui directory
DB_PATH = Path("../data/crawler.db")

def get_db_connection():
    """Create and return a SQLite database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Enable column access by name
    return conn

@app.route('/')
def index():
    """Display summary dashboard."""
    stats = get_summary_stats()
    alerts = get_recent_alerts()
    failures = get_recent_failures()
    return render_template('summary.html', stats=stats, alerts=alerts, failures=failures)

@app.route('/urls')
def urls():
    """Display crawl overview from urls table."""
    return render_template('urls.html', urls=get_urls())

def get_urls():
    """Fetch all data from urls table."""
    conn = get_db_connection()
    urls = conn.execute('SELECT * FROM urls').fetchall()
    conn.close()
    return urls

@app.route('/baselines')
def baselines():
    """Display baseline overview from baseline table."""
    return render_template('baselines.html', baselines=get_baselines())

def get_baselines():
    """Fetch all data from baseline table, shorten html_hash."""
    conn = get_db_connection()
    baselines = conn.execute('SELECT id, url, substr(html_hash, 1, 8) as html_hash_short, script_sources, baseline_created_at, baseline_updated_at FROM baseline').fetchall()
    conn.close()
    return baselines

@app.route('/alerts')
def alerts():
    """Display defacement alerts from diff_evidence table where status='open', sorted by detected_at DESC."""
    return render_template('alerts.html', alerts=get_alerts())

@app.route('/alert/<int:alert_id>')
def alert_detail(alert_id):
    """Display detailed information for a specific alert."""
    conn = get_db_connection()
    alert = conn.execute("SELECT * FROM diff_evidence WHERE id = ?", (alert_id,)).fetchone()
    conn.close()
    if alert:
        alert_dict = dict(alert)

        # Convert timestamp to IST
        if alert_dict['detected_at']:
            utc_time = datetime.fromisoformat(alert_dict['detected_at'].replace('Z', '+00:00'))
            ist_time = utc_time + timedelta(hours=5, minutes=30)
            alert_dict['detected_at_ist'] = ist_time.strftime('%Y-%m-%d %H:%M:%S IST')

        # Pretty-print diff_summary JSON
        if alert['diff_summary']:
            try:
                alert_dict['diff_summary_pretty'] = json.dumps(json.loads(alert['diff_summary']), indent=2)
            except json.JSONDecodeError:
                alert_dict['diff_summary_pretty'] = alert['diff_summary']
        else:
            alert_dict['diff_summary_pretty'] = ''

        # Fetch baseline and observed HTML content
        baseline_html = get_baseline_html(alert_dict['baseline_hash'])

        # Try to get observed HTML from diff_summary path first, then fallback to hash lookup
        observed_html = get_observed_html_from_diff_summary(alert_dict.get('diff_summary'))
        if not observed_html:
            observed_html = get_observed_html(alert_dict['observed_hash'])

        # Add line numbers and highlighting to HTML content
        if baseline_html and observed_html:
            baseline_highlighted = add_line_numbers_with_highlighting(baseline_html, observed_html, 'baseline')
            observed_highlighted = add_line_numbers_with_highlighting(observed_html, baseline_html, 'observed')
        else:
            baseline_highlighted = add_line_numbers(baseline_html) if baseline_html else "Baseline HTML not available"
            observed_highlighted = add_line_numbers(observed_html) if observed_html else "Observed HTML not available"

        return render_template('alert_detail.html',
                             alert=alert_dict,
                             baseline_html=baseline_highlighted,
                             observed_html=observed_highlighted)
    else:
        return "Alert not found", 404

def get_alerts():
    """Fetch open alerts, sorted by detected_at DESC."""
    conn = get_db_connection()
    alerts = conn.execute("SELECT * FROM diff_evidence WHERE status = 'open' ORDER BY detected_at DESC").fetchall()
    # Pretty-print diff_summary JSON
    for alert in alerts:
        if alert['diff_summary']:
            try:
                alert_dict = dict(alert)
                alert_dict['diff_summary_pretty'] = json.dumps(json.loads(alert['diff_summary']), indent=2)
            except json.JSONDecodeError:
                alert_dict['diff_summary_pretty'] = alert['diff_summary']
        else:
            alert_dict = dict(alert)
            alert_dict['diff_summary_pretty'] = ''
    conn.close()
    return alerts

def get_summary_stats():
    """Fetch summary statistics for the dashboard."""
    conn = get_db_connection()
    stats = {}
    stats['total_urls'] = conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0]
    stats['crawled_urls'] = conn.execute("SELECT COUNT(*) FROM urls WHERE status = 'crawled'").fetchone()[0]
    stats['fetch_failures'] = conn.execute("SELECT COUNT(*) FROM urls WHERE status = 'fetch_failed'").fetchone()[0]
    stats['baselines_created'] = conn.execute("SELECT COUNT(*) FROM baseline").fetchone()[0]
    stats['open_alerts'] = conn.execute("SELECT COUNT(*) FROM diff_evidence WHERE status = 'open'").fetchone()[0]
    stats['high_severity'] = conn.execute("SELECT COUNT(*) FROM diff_evidence WHERE severity = 'HIGH'").fetchone()[0]
    stats['medium_severity'] = conn.execute("SELECT COUNT(*) FROM diff_evidence WHERE severity = 'MEDIUM'").fetchone()[0]
    conn.close()
    return stats

def get_recent_alerts(limit=5):
    """Fetch recent open alerts."""
    conn = get_db_connection()
    alerts = conn.execute("SELECT url, severity, detected_at FROM diff_evidence WHERE status = 'open' ORDER BY detected_at DESC LIMIT ?", (limit,)).fetchall()
    conn.close()

    # Convert timestamps to IST
    from datetime import datetime, timezone, timedelta
    for alert in alerts:
        if alert['detected_at']:
            utc_time = datetime.fromisoformat(alert['detected_at'].replace('Z', '+00:00'))
            ist_time = utc_time + timedelta(hours=5, minutes=30)
            alert_dict = dict(alert)
            alert_dict['detected_at_ist'] = ist_time.strftime('%Y-%m-%d   %H:%M:%S IST')
            # Replace the tuple with dict for template access
            alerts = [alert_dict if a == alert else a for a in alerts]

    return alerts

def get_recent_failures(limit=5):
    """Fetch recent fetch failures."""
    conn = get_db_connection()
    failures = conn.execute("SELECT url, last_crawled_at FROM urls WHERE status = 'fetch_failed' ORDER BY last_crawled_at DESC LIMIT ?", (limit,)).fetchall()
    conn.close()

    # Convert timestamps to IST
    from datetime import datetime, timezone, timedelta
    for failure in failures:
        if failure['last_crawled_at']:
            utc_time = datetime.fromisoformat(failure['last_crawled_at'].replace('Z', '+00:00'))
            ist_time = utc_time + timedelta(hours=5, minutes=30)
            failure_dict = dict(failure)
            failure_dict['last_crawled_at_ist'] = ist_time.strftime('%Y-%m-%d   %H:%M:%S IST')
            # Replace the tuple with dict for template access
            failures = [failure_dict if f == failure else f for f in failures]

    return failures

def get_baseline_html(hashval):
    """Fetch baseline HTML content from snapshots."""
    if not hashval:
        return None
    from crawler.config import DATA_DIR
    path = os.path.join(DATA_DIR, "snapshots", "baselines", f"baseline_{hashval}.html")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            return fh.read()
    except Exception:
        return None


def get_observed_html(hashval):
    """Fetch observed HTML content from snapshots."""
    if not hashval:
        return None
    from crawler.config import DATA_DIR
    path = os.path.join(DATA_DIR, "snapshots", "observed", f"observed_{hashval}.html")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            return fh.read()
    except Exception:
        return None


def get_observed_html_from_diff_summary(diff_summary_json):
    """Fetch observed HTML content using the path stored in diff_summary."""
    if not diff_summary_json:
        return None

    try:
        diff_data = json.loads(diff_summary_json)
        snapshot_path = diff_data.get('observed_snapshot_path')
        if snapshot_path and os.path.exists(snapshot_path):
            with open(snapshot_path, "r", encoding="utf-8", errors="ignore") as fh:
                return fh.read()
    except (json.JSONDecodeError, IOError):
        pass

    return None


def add_line_numbers(html_content):
    """Add line numbers to HTML content like VS Code."""
    if not html_content:
        return html_content

    lines = html_content.splitlines()
    numbered_lines = []

    for i, line in enumerate(lines, 1):
        # Format line number with consistent width (pad to 4 digits)
        line_num = f"{i:4d}"
        numbered_lines.append(f"{line_num} | {line}")

    return '\n'.join(numbered_lines)


def add_line_numbers_with_highlighting(html_content, other_html, mode):
    """Add line numbers and color highlighting based on diff comparison."""
    if not html_content or not other_html:
        return add_line_numbers(html_content)

    import html

    content_lines = html_content.splitlines()
    other_lines = other_html.splitlines()

    # Use difflib to find differences
    matcher = difflib.SequenceMatcher(None, content_lines, other_lines)
    highlighted_lines = []

    line_number = 1

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            # No changes - add lines normally (no highlighting)
            for line in content_lines[i1:i2]:
                line_num = f"{line_number:4d}"
                escaped_line = html.escape(line)
                highlighted_lines.append(f'<div class="line-normal">{line_num} | {escaped_line}</div>')
                line_number += 1
        elif tag == 'delete' and mode == 'baseline':
            # Lines removed from baseline - highlight in red
            for line in content_lines[i1:i2]:
                line_num = f"{line_number:4d}"
                escaped_line = html.escape(line)
                highlighted_lines.append(f'<div class="line-removed">{line_num} | {escaped_line}</div>')
                line_number += 1
        elif tag == 'insert' and mode == 'observed':
            # Lines added to observed - highlight in green
            for line in content_lines[i1:i2]:
                line_num = f"{line_number:4d}"
                escaped_line = html.escape(line)
                highlighted_lines.append(f'<div class="line-added">{line_num} | {escaped_line}</div>')
                line_number += 1
        elif tag == 'replace':
            # Lines changed - highlight in yellow (for edited lines)
            for line in content_lines[i1:i2]:
                line_num = f"{line_number:4d}"
                escaped_line = html.escape(line)
                highlighted_lines.append(f'<div class="line-changed">{line_num} | {escaped_line}</div>')
                line_number += 1
        else:
            # For other cases, add lines normally
            for line in content_lines[i1:i2]:
                line_num = f"{line_number:4d}"
                escaped_line = html.escape(line)
                highlighted_lines.append(f"{line_num} | {escaped_line}")
                line_number += 1

    return '\n'.join(highlighted_lines)


# canonicalize_html is defined in main.py for canonical hashing; do not duplicate here


def generate_unified_diff(baseline_html, observed_html):
    """Generate unified diff between baseline and observed HTML."""
    if not baseline_html or not observed_html:
        return "Diff not available - missing HTML content"

    baseline_lines = baseline_html.splitlines(keepends=True)
    observed_lines = observed_html.splitlines(keepends=True)

    diff = list(difflib.unified_diff(
        baseline_lines,
        observed_lines,
        fromfile='baseline.html',
        tofile='observed.html',
        lineterm='',
        n=3
    ))

    return ''.join(diff)


def highlight_code_changes(html_content, other_html, mode):
    """Highlight changes in HTML content based on comparison with other version."""
    if not html_content or not other_html:
        return html_content or "Content not available"

    content_lines = html_content.splitlines()
    other_lines = other_html.splitlines() if other_html else []

    # Use difflib to find differences
    matcher = difflib.SequenceMatcher(None, content_lines, other_lines)
    highlighted_lines = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            # No changes - add lines normally
            for line in content_lines[i1:i2]:
                highlighted_lines.append(f'<div class="line-normal">{line}</div>')
        elif tag == 'delete' and mode == 'baseline':
            # Lines removed from baseline - highlight in red
            for line in content_lines[i1:i2]:
                highlighted_lines.append(f'<div class="line-removed">{line}</div>')
        elif tag == 'insert' and mode == 'observed':
            # Lines added to observed - highlight in green
            for line in content_lines[i1:i2]:
                highlighted_lines.append(f'<div class="line-added">{line}</div>')
        elif tag == 'replace':
            # Lines changed - highlight in yellow
            for line in content_lines[i1:i2]:
                highlighted_lines.append(f'<div class="line-changed">{line}</div>')
        else:
            # For other cases, add lines normally
            for line in content_lines[i1:i2]:
                highlighted_lines.append(f'<div class="line-normal">{line}</div>')

    return '\n'.join(highlighted_lines)


if __name__ == '__main__':
    app.run(debug=True)

from flask import Flask, render_template
import sqlite3
import json
from pathlib import Path

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
    """Redirect to urls page as default."""
    return render_template('urls.html', urls=get_urls())

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

if __name__ == '__main__':
    app.run(debug=True)

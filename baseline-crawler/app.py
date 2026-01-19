"""
New Flask UI for Web Crawler
Dashboard for crawl summary, failed URLs, baselines, and defacement detection
"""

from flask import Flask, render_template, jsonify, request
import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime, timezone, timedelta
import json

app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), 'templates'))

# Database connection pool
def get_db_connection():
    """Create MySQL database connection"""
    try:
        conn = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', ''),
            database=os.getenv('DB_NAME', 'crawlerdb')
        )
        return conn
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None

# ============================================================
# DASHBOARD - Summary Statistics
# ============================================================

@app.route('/')
def dashboard():
    """Main dashboard with crawl summary"""
    conn = get_db_connection()
    if not conn:
        return "Database connection failed", 500
    
    cursor = conn.cursor(dictionary=True)
    
    stats = {}
    
    try:
        # Total crawl jobs
        cursor.execute("SELECT COUNT(*) as count FROM crawl_jobs")
        stats['total_jobs'] = cursor.fetchone()['count']
        
        # Completed jobs
        cursor.execute("SELECT COUNT(*) as count FROM crawl_jobs WHERE status='completed'")
        stats['completed_jobs'] = cursor.fetchone()['count']
        
        # Failed jobs
        cursor.execute("SELECT COUNT(*) as count FROM crawl_jobs WHERE status='failed'")
        stats['failed_jobs'] = cursor.fetchone()['count']
        
        # Total pages crawled
        cursor.execute("SELECT SUM(pages_crawled) as total FROM crawl_jobs WHERE status='completed'")
        result = cursor.fetchone()
        stats['total_pages_crawled'] = result['total'] or 0
        
        # Total baselines stored
        cursor.execute("SELECT COUNT(*) as count FROM baselines")
        stats['total_baselines'] = cursor.fetchone()['count']
        
        # Open defacement alerts
        cursor.execute("SELECT COUNT(*) as count FROM diff_evidence WHERE status='open'")
        stats['open_alerts'] = cursor.fetchone()['count']
        
        # High severity alerts
        cursor.execute("SELECT COUNT(*) as count FROM diff_evidence WHERE status='open' AND severity='HIGH'")
        stats['high_severity'] = cursor.fetchone()['count']
        
        # Medium severity alerts
        cursor.execute("SELECT COUNT(*) as count FROM diff_evidence WHERE status='open' AND severity='MEDIUM'")
        stats['medium_severity'] = cursor.fetchone()['count']
        
        # Recent crawl jobs (last 5)
        cursor.execute("""
            SELECT job_id, custid, siteid, start_url, status, pages_crawled, 
                   started_at, completed_at
            FROM crawl_jobs 
            ORDER BY started_at DESC 
            LIMIT 5
        """)
        stats['recent_jobs'] = cursor.fetchall()
        
    finally:
        cursor.close()
        conn.close()
    
    return render_template('dashboard.html', stats=stats)

# ============================================================
# FAILED URLs - Pages that failed to fetch
# ============================================================

@app.route('/failed-urls')
def failed_urls():
    """Page for viewing failed URL crawls"""
    conn = get_db_connection()
    if not conn:
        return "Database connection failed", 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT id, url, parent_url, status_code, content_type, 
                   response_time_ms, fetched_at, job_id
            FROM crawl_pages 
            WHERE status_code IS NULL OR status_code >= 400 
            ORDER BY fetched_at DESC
            LIMIT 100
        """)
        failed = cursor.fetchall()
        
        # Convert timestamps to IST
        for item in failed:
            if item['fetched_at']:
                utc_time = datetime.fromisoformat(str(item['fetched_at']).replace('Z', '+00:00'))
                ist_time = utc_time + timedelta(hours=5, minutes=30)
                item['fetched_at_ist'] = ist_time.strftime('%Y-%m-%d %H:%M:%S IST')
    
    finally:
        cursor.close()
        conn.close()
    
    return render_template('failed_urls.html', failed_urls=failed)

# ============================================================
# BASELINES - Stored baseline snapshots
# ============================================================

@app.route('/baselines')
def baselines():
    """Page for viewing stored baselines"""
    conn = get_db_connection()
    if not conn:
        return "Database connection failed", 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT id, siteid, url, substr(html_hash, 1, 8) as html_hash_short, 
                   snapshot_path, baseline_created_at, baseline_updated_at
            FROM baselines 
            ORDER BY baseline_created_at DESC
            LIMIT 100
        """)
        baseline_list = cursor.fetchall()
        
        # Convert timestamps to IST
        for item in baseline_list:
            if item['baseline_created_at']:
                utc_time = datetime.fromisoformat(str(item['baseline_created_at']).replace('Z', '+00:00'))
                ist_time = utc_time + timedelta(hours=5, minutes=30)
                item['baseline_created_at_ist'] = ist_time.strftime('%Y-%m-%d %H:%M:%S IST')
            
            if item['baseline_updated_at']:
                utc_time = datetime.fromisoformat(str(item['baseline_updated_at']).replace('Z', '+00:00'))
                ist_time = utc_time + timedelta(hours=5, minutes=30)
                item['baseline_updated_at_ist'] = ist_time.strftime('%Y-%m-%d %H:%M:%S IST')
    
    finally:
        cursor.close()
        conn.close()
    
    return render_template('baselines.html', baselines=baseline_list)

# ============================================================
# DEFACEMENT DETECTION - Detected changes
# ============================================================

@app.route('/detections')
def detections():
    """Page for viewing defacement detection results"""
    conn = get_db_connection()
    if not conn:
        return "Database connection failed", 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT id, siteid, url, baseline_hash, observed_hash, severity, 
                   status, detected_at, closed_at
            FROM diff_evidence 
            WHERE status='open' 
            ORDER BY detected_at DESC
            LIMIT 100
        """)
        detections_list = cursor.fetchall()
        
        # Convert timestamps to IST
        for item in detections_list:
            if item['detected_at']:
                utc_time = datetime.fromisoformat(str(item['detected_at']).replace('Z', '+00:00'))
                ist_time = utc_time + timedelta(hours=5, minutes=30)
                item['detected_at_ist'] = ist_time.strftime('%Y-%m-%d %H:%M:%S IST')
    
    finally:
        cursor.close()
        conn.close()
    
    return render_template('detections.html', detections=detections_list)

# ============================================================
# DETECTION DETAIL - Full defacement info
# ============================================================

@app.route('/detection/<int:detection_id>')
def detection_detail(detection_id):
    """Page for viewing full defacement detection details"""
    conn = get_db_connection()
    if not conn:
        return "Database connection failed", 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT id, siteid, url, baseline_hash, observed_hash, diff_summary, 
                   severity, status, detected_at, closed_at
            FROM diff_evidence 
            WHERE id = %s
        """, (detection_id,))
        detection = cursor.fetchone()
        
        if not detection:
            return "Detection not found", 404
        
        # Convert timestamp to IST
        if detection['detected_at']:
            utc_time = datetime.fromisoformat(str(detection['detected_at']).replace('Z', '+00:00'))
            ist_time = utc_time + timedelta(hours=5, minutes=30)
            detection['detected_at_ist'] = ist_time.strftime('%Y-%m-%d %H:%M:%S IST')
        
        # Parse diff_summary
        if detection['diff_summary']:
            try:
                detection['diff_summary_json'] = json.loads(detection['diff_summary'])
            except:
                detection['diff_summary_json'] = {}
    
    finally:
        cursor.close()
        conn.close()
    
    return render_template('detection_detail.html', detection=detection)

# ============================================================
# API ENDPOINTS
# ============================================================

@app.route('/api/stats')
def api_stats():
    """JSON API for dashboard stats"""
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    cursor = conn.cursor(dictionary=True)
    stats = {}
    
    try:
        cursor.execute("SELECT COUNT(*) as count FROM crawl_jobs WHERE status='completed'")
        stats['completed_jobs'] = cursor.fetchone()['count']
        
        cursor.execute("SELECT SUM(pages_crawled) as total FROM crawl_jobs WHERE status='completed'")
        result = cursor.fetchone()
        stats['total_pages_crawled'] = result['total'] or 0
        
        cursor.execute("SELECT COUNT(*) as count FROM diff_evidence WHERE status='open'")
        stats['open_alerts'] = cursor.fetchone()['count']
    
    finally:
        cursor.close()
        conn.close()
    
    return jsonify(stats)

# ============================================================
# ERROR HANDLERS
# ============================================================

@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def server_error(error):
    return render_template('500.html'), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

"""
MySQL database operations for the web crawler.
All crawl data, baseline storage, and comparison results are written here.
"""

import mysql.connector
import logging
from datetime import datetime, timezone
from ..config import DB_CONFIG

logger = logging.getLogger(__name__)


def get_connection():
    """Get a MySQL connection from the connection pool."""
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except Exception as e:
        logger.error(f"Failed to connect to MySQL: {e}")
        raise


def check_db_health() -> bool:
    """
    Check if MySQL database is accessible and has required tables.
    Returns True if healthy, False otherwise.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Check if sites table exists
        cursor.execute("SELECT COUNT(*) FROM sites")
        cursor.fetchone()
        
        cursor.close()
        conn.close()
        logger.info("✅ MySQL health check passed")
        return True
    except Exception as e:
        logger.error(f"❌ MySQL health check failed: {e}")
        return False


def fetch_enabled_sites() -> list:
    """
    Fetch all enabled sites from the sites table.
    Returns list of dicts: [{"siteid": int, "custid": int, "url": str}, ...]
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(
            "SELECT siteid, custid, url FROM sites WHERE enabled = 1 ORDER BY siteid"
        )
        sites = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Fetched {len(sites)} enabled site(s)")
        return sites
    except Exception as e:
        logger.error(f"❌ Failed to fetch enabled sites: {e}")
        return []


def insert_crawl_job(job_id: str, custid: int, siteid: int, start_url: str) -> bool:
    """
    Insert a new crawl job record.
    Normalizes start_url to remove trailing slashes.
    Returns True if successful, False otherwise.
    """
    try:
        from ..normalizer import normalize_url
        # Remove trailing slashes from start_url
        normalized_url = normalize_url(start_url)
        
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """INSERT INTO crawl_jobs (job_id, custid, siteid, start_url, status, started_at)
               VALUES (%s, %s, %s, %s, 'running', NOW())""",
            (job_id, custid, siteid, normalized_url)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Inserted crawl_job: {job_id} for site {siteid} ({normalized_url})")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to insert crawl_job: {e}")
        return False


def complete_crawl_job(job_id: str, pages_crawled: int) -> bool:
    """
    Mark a crawl job as completed.
    Returns True if successful, False otherwise.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """UPDATE crawl_jobs 
               SET status = 'completed', pages_crawled = %s, completed_at = NOW()
               WHERE job_id = %s""",
            (pages_crawled, job_id)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Marked crawl_job {job_id} as completed ({pages_crawled} pages)")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to complete crawl_job: {e}")
        return False


def fail_crawl_job(job_id: str, error_message: str) -> bool:
    """
    Mark a crawl job as failed.
    Returns True if successful, False otherwise.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """UPDATE crawl_jobs 
               SET status = 'failed', error_msg = %s, completed_at = NOW()
               WHERE job_id = %s""",
            (error_message, job_id)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"❌ Marked crawl_job {job_id} as failed: {error_message}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to fail crawl_job: {e}")
        return False


def insert_crawl_page(data: dict) -> bool:
    """
    Insert a crawl page record.
    
    Expected data dict keys:
    - job_id, custid, siteid, url, status_code, content_type
    
    Returns True if successful, False otherwise.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """INSERT INTO crawl_pages 
               (job_id, custid, siteid, url, status_code, content_type)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (
                data.get("job_id"),
                data.get("custid"),
                data.get("siteid"),
                data.get("url"),
                data.get("status_code"),
                data.get("content_type")
            )
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.debug(f"✅ Inserted crawl_page: {data.get('url')}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to insert crawl_page: {e}")
        return False


def insert_baseline(data: dict) -> bool:
    """
    Insert a baseline snapshot record.
    
    Expected data dict keys:
    - siteid, url, html_hash, storage_path
    
    Returns True if successful, False otherwise.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """INSERT INTO baselines 
               (siteid, url, html_hash, snapshot_path, baseline_created_at)
               VALUES (%s, %s, %s, %s, NOW())""",
            (
                data.get("siteid"),
                data.get("url"),
                data.get("html_hash"),
                data.get("storage_path")
            )
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.debug(f"✅ Inserted baseline: {data.get('url')}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to insert baseline: {e}")
        return False


def insert_diff(data: dict) -> bool:
    """
    Insert a diff_evidence record (detected defacement).
    
    Expected data dict keys:
    - custid, siteid, url, baseline_id, status, severity, diff_location, diff_snippet
    
    Returns True if successful, False otherwise.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """INSERT INTO diff_evidence 
               (custid, siteid, url, baseline_id, status, severity, diff_location, diff_snippet, detected_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
            (
                data.get("custid"),
                data.get("siteid"),
                data.get("url"),
                data.get("baseline_id"),
                data.get("status", "open"),
                data.get("severity", "medium"),
                data.get("diff_location"),
                data.get("diff_snippet")
            )
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.debug(f"✅ Inserted diff_evidence: {data.get('url')}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to insert diff: {e}")
        return False

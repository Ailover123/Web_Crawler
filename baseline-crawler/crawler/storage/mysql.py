from mysql.connector.pooling import MySQLConnectionPool
from mysql.connector import Error
import os
from dotenv import load_dotenv
from crawler.storage.db_guard import DB_SEMAPHORE
from crawler.normalizer import get_canonical_id

load_dotenv()


def with_retry(func):
    """Placeholder retry decorator (currently no-op)."""
    return func


pool = MySQLConnectionPool(
    pool_name="crawler_pool",
    pool_size=int(os.getenv("MYSQL_POOL_SIZE", 5)),
    host=os.getenv("MYSQL_HOST"),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("MYSQL_DATABASE"),
)


def get_connection():
    DB_SEMAPHORE.acquire()
    try:
        return pool.get_connection()
    except Exception:
        DB_SEMAPHORE.release()
        raise


def check_db_health():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        return cur.fetchone()[0] == 1
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


def fetch_enabled_sites():
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT siteid, custid, url FROM sites WHERE enabled=1")
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


@with_retry
def insert_crawl_job(job_id, custid, siteid, start_url=None):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO crawl_jobs (
                job_id,
                custid,
                siteid,
                status
            ) VALUES (%s, %s, %s, 'running')
            """,
            (job_id, custid, siteid),
        )
        conn.commit()
    except Error:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
        DB_SEMAPHORE.release()


def complete_crawl_job(job_id, pages_crawled=None):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE crawl_jobs SET status='completed' WHERE job_id=%s",
            (job_id,),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


def fail_crawl_job(job_id, err):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE crawl_jobs SET status='failed' WHERE job_id=%s",
            (job_id,),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


def insert_crawl_page(data):
    # Pass seed_url if available to ensure domain matches sites table
    base_url = data.get("base_url")
    canonical_url = get_canonical_id(data["url"], base_url)
    if not canonical_url:
        return

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO crawl_pages
            (job_id, custid, siteid, url, parent_url, depth, status_code,
             content_type, content_length, response_time_ms, fetched_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                job_id=VALUES(job_id),
                parent_url=VALUES(parent_url),
                depth=VALUES(depth),
                status_code=VALUES(status_code),
                content_type=VALUES(content_type),
                content_length=VALUES(content_length),
                response_time_ms=VALUES(response_time_ms),
                fetched_at=VALUES(fetched_at)
            """,
            (
                data["job_id"], data["custid"], data["siteid"],
                canonical_url, data["parent_url"], data["depth"],
                data["status_code"], data["content_type"],
                data["content_length"], data["response_time_ms"],
                data["fetched_at"],
            ),
        )
        conn.commit()
        return "Inserted" if cur.rowcount == 1 else "Updated"
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


def insert_defacement_site(siteid, baseline_id, url, base_url=None):
    canonical_url = get_canonical_id(url, base_url)
    if not canonical_url:
        return

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO defacement_sites (siteid, baseline_id, url, action)
            VALUES (%s,%s,%s,'selected')
            ON DUPLICATE KEY UPDATE
                baseline_id=VALUES(baseline_id),
                action='selected'
            """,
            (siteid, baseline_id, canonical_url),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


# ============================================================
# BASELINE HASH HELPERS (IMMUTABLE)
# ============================================================

def upsert_baseline_hash(site_id, normalized_url, content_hash, baseline_path, base_url=None):
    """
    Baseline pages are INSERTED ONCE and NEVER UPDATED from the crawler.
    Uses INSERT IGNORE to protect existing baselines.
    Returns:
        bool: True if inserted, False if duplicate/ignored.
    """
    canonical_url = get_canonical_id(normalized_url, base_url)
    if not canonical_url:
        return False

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT IGNORE INTO baseline_pages
                (site_id, normalized_url, content_hash, baseline_path)
            VALUES (%s, %s, %s, %s)
            """,
            (site_id, canonical_url, content_hash, baseline_path),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


def fetch_baseline_hash(site_id, normalized_url, base_url=None):
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        # Use canonical "Domain/Path" for lookup
        canonical_url = get_canonical_id(normalized_url, base_url)
        cur.execute(
            """
            SELECT content_hash
            FROM baseline_pages
            WHERE site_id=%s AND normalized_url=%s
            """,
            (site_id, canonical_url),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


# ============================================================
# OBSERVED PAGE UPSERT (STATE, NOT HISTORY)
# ============================================================

def insert_observed_page(
    site_id,
    baseline_id,
    normalized_url,
    observed_hash,
    changed,
    diff_path=None,
    defacement_score=None,
    defacement_severity=None,
    base_url=None,
):
    canonical_url = get_canonical_id(normalized_url, base_url)
    if not canonical_url:
        return

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO observed_pages
                (site_id, baseline_id, normalized_url,
                 observed_hash, changed, diff_path,
                 defacement_score, defacement_severity)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                baseline_id = VALUES(baseline_id),
                observed_hash = VALUES(observed_hash),
                changed = VALUES(changed),
                diff_path = VALUES(diff_path),
                defacement_score = VALUES(defacement_score),
                defacement_severity = VALUES(defacement_severity)
            """,
            (
                site_id,
                baseline_id,
                canonical_url,
                observed_hash,
                changed,
                diff_path,
                defacement_score,
                defacement_severity,
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()

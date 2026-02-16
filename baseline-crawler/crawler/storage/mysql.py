from mysql.connector.pooling import MySQLConnectionPool
from mysql.connector import Error
import os
import mysql.connector
import threading
from dotenv import load_dotenv
from crawler.storage.db_guard import DB_SEMAPHORE
from crawler.processor import LinkUtility

load_dotenv(override=True)


def with_retry(func):
    """Placeholder retry decorator (currently no-op)."""
    return func

pool_size = int(os.getenv("MYSQL_POOL_SIZE", 5))
db_host = os.getenv("MYSQL_HOST")
db_user = os.getenv("MYSQL_USER")
db_password = os.getenv("MYSQL_PASSWORD")
db_name = os.getenv("MYSQL_DATABASE")
db_port = os.getenv("MYSQL_PORT", 3306)

pool = MySQLConnectionPool(
    pool_name="crawler_pool",
    pool_size=pool_size,
    host=db_host,
    port=db_port,
    user=db_user,
    password=db_password,
    database=db_name,
)


import time
from crawler.core import logger

def get_connection(timeout: int = 10):
    """
    Acquire a DB connection with a timeout.
    Prevents silent deadlocks when DB_SEMAPHORE is exhausted.
    """
    acquired = DB_SEMAPHORE.acquire(timeout=timeout)

    if not acquired:
        logger.error(
            "[DB] Semaphore acquire timeout after "
            f"{timeout}s — possible connection starvation or deadlock"
        )
        raise RuntimeError(
            "DB semaphore timeout: too many concurrent DB operations"
        )

    try:
        return pool.get_connection()
    except Exception:
        DB_SEMAPHORE.release()
        raise



def ensure_baseline_columns(conn):
    """
    Ensures defacement_sites has the necessary columns for baseline storage.
    Migration helper.
    """
    try:
        cur = conn.cursor()
        
        # Add columns if missing to defacement_sites
        cols = [
            ("content_hash", "CHAR(64) NULL"),
            ("baseline_path", "VARCHAR(1024) NULL"),
            ("baseline_id", "VARCHAR(255) NULL"),
            ("updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
        ]
        
        for col_name, col_def in cols:
            # Check if column exists
            cur.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'defacement_sites'
                  AND COLUMN_NAME = %s
                """,
                (col_name,)
            )
            if cur.fetchone()[0] == 0:
                logger.info(f"[MIGRATION] Adding column {col_name} to defacement_sites...")
                cur.execute(f"ALTER TABLE defacement_sites ADD COLUMN {col_name} {col_def}")
        # -------------------------------------------------------------
        # SCHEMA REPAIR: Fix crawl_jobs Foreign Key (sites_old -> sites)
        # -------------------------------------------------------------
        # 1. Check if sites.siteid is indexed
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.STATISTICS 
            WHERE TABLE_SCHEMA = DATABASE() 
              AND TABLE_NAME = 'sites' 
              AND COLUMN_NAME = 'siteid'
            """
        )
        if cur.fetchone()[0] == 0:
            logger.info("[SCHEMA REPAIR] Creating index idx_siteid on sites table...")
            cur.execute("CREATE INDEX idx_siteid ON sites(siteid)")

        # 2. Check if crawl_jobs references sites_old
        cur.execute(
            """
            SELECT CONSTRAINT_NAME 
            FROM information_schema.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = DATABASE() 
              AND TABLE_NAME = 'crawl_jobs' 
              AND REFERENCED_TABLE_NAME = 'sites_old'
            """
        )
        old_fk = cur.fetchone()
        
        if old_fk:
            fk_name = old_fk[0]  # e.g., crawl_jobs_ibfk_1
            logger.info(f"[SCHEMA REPAIR] Found obsolete FK {fk_name} (crawl_jobs -> sites_old). Dropping...")
            cur.execute(f"ALTER TABLE crawl_jobs DROP FOREIGN KEY {fk_name}")
            
            # 3. Add new Foreign Key (crawl_jobs -> sites)
            logger.info("[SCHEMA REPAIR] Adding new FK (crawl_jobs -> sites)...")
            # Ensure no orphan jobs exist before adding constraint
            cur.execute("DELETE FROM crawl_jobs WHERE siteid NOT IN (SELECT siteid FROM sites)")
            cur.execute(
                """
                ALTER TABLE crawl_jobs 
                ADD CONSTRAINT crawl_jobs_fk_sites 
                FOREIGN KEY (siteid) REFERENCES sites(siteid) 
                ON DELETE CASCADE
                """
            )
        
        conn.commit()
    except Exception as e:
        logger.error(f"[MIGRATION] Failed to ensure schema: {e}")
        # Don't raise, allow startup to proceed/fail naturally later


def check_db_health():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        res = cur.fetchone()
        is_healthy = res[0] == 1
        
        # Run schema check on health check
        ensure_baseline_columns(conn)
        
        return is_healthy
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


def fetch_enabled_sites():
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT siteid, custid, url FROM sites WHERE enabled = 1")
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
            SELECT 1
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'crawl_jobs'
              AND COLUMN_NAME = 'start_url'
            LIMIT 1
            """
        )
        has_start_url = cursor.fetchone() is not None

        # BYPASS FK CHECK: sites_prod (parent) is missing data from sites (child source).
        # We must disable FK checks to allow crawl_jobs to link to sites IDs.
        cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        if has_start_url:
            cursor.execute(
                """
                INSERT INTO crawl_jobs (
                    job_id,
                    custid,
                    siteid,
                    start_url,
                    status
                ) VALUES (%s, %s, %s, %s, 'running')
                """,
                (job_id, custid, siteid, start_url),
            )
        else:
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
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
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
    # 🛡️ Safety check: Prevent any crawl_pages insertions during BASELINE or COMPARE mode
    crawl_mode = os.getenv("CRAWL_MODE", "CRAWL").upper()
    if crawl_mode in ("BASELINE", "COMPARE"):
        from crawler.core import logger
        logger.warning(
            f"[SAFETY] Attempted to insert into crawl_pages during {crawl_mode} mode. "
            f"This operation is prohibited. URL: {data.get('url')}"
        )
        return None

    # Pass seed_url if available to ensure domain matches sites table
    base_url = data.get("base_url")
    canonical_url = LinkUtility.get_canonical_id(data["url"], base_url)
    if not canonical_url:
        return None

    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # 1. 🛡️ Check if the page already exists for this site
        # We use (siteid, url) which matches the UNIQUE KEY unique_site_page
        cur.execute(
            "SELECT id FROM crawl_pages WHERE siteid = %s AND url = %s",
            (data["siteid"], canonical_url)
        )
        row = cur.fetchone()

        if row:
            # 2. 🛡️ INSERT-ONLY: Exists, so we don't update anything.
            # We return 'Existed' so the worker knows it wasn't a fresh insert.
            action = "Existed"
            affected_id = row[0]
        else:
            # 3. 🆕 INSERT: Doesn't exist, so insert fresh
            try:
                cur.execute(
                    """
                    INSERT INTO crawl_pages
                    (job_id, custid, siteid, url, parent_url, depth, status_code,
                     content_type, content_length, response_time_ms, fetched_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        data["job_id"], data["custid"], data["siteid"],
                        canonical_url, data["parent_url"], data["depth"],
                        data["status_code"], data["content_type"],
                        data["content_length"], data["response_time_ms"],
                        data["fetched_at"]
                    )
                )
                action = "Inserted"
                affected_id = cur.lastrowid
            except mysql.connector.errors.DataError as e:
                if e.errno == 1406: # Data too long
                    from crawler.core import logger
                    logger.error(f"[DB] Skipping insertion: URL too long for database column ({len(canonical_url)} chars)")
                    return None
                raise e
            except mysql.connector.errors.IntegrityError as e:
                if e.errno == 1062: # Duplicate entry
                    # 🛡️ RACE CONDITION: Another worker inserted this URL since our SELECT
                    action = "Existed"
                    cur.execute(
                        "SELECT id FROM crawl_pages WHERE siteid = %s AND url = %s",
                        (data["siteid"], canonical_url)
                    )
                    row = cur.fetchone()
                    affected_id = row[0] if row else 0
                else:
                    raise e

        conn.commit()

        if action == "Inserted":
            from crawler.core import logger
            logger.info(f"DB: Inserted {canonical_url} (ID: {affected_id})")
        elif action == "Updated":
            from crawler.core import logger
            logger.info(f"DB: Updated {canonical_url} (ID: {affected_id})")

        return {
            "action": action,
            "id": affected_id
        }
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


def insert_defacement_site(siteid, baseline_id, url, base_url=None):
    canonical_url = LinkUtility.get_canonical_id(url, base_url)
    if not canonical_url:
        return

    conn = get_connection()
    try:
        cur = conn.cursor(buffered=True)
        
        # 1. Manual Existence Check
        cur.execute(
            "SELECT id FROM defacement_sites WHERE siteid = %s AND url = %s",
            (siteid, canonical_url)
        )
        row = cur.fetchone()
        try:
             cur.fetchall()
        except:
             pass
        
        if row:
            cur.execute(
                "UPDATE defacement_sites SET action = 'selected', baseline_id = %s WHERE id = %s",
                (baseline_id, row[0])
            )
        else:
            cur.execute(
                """
                INSERT INTO defacement_sites (siteid, url, baseline_id, action)
                VALUES (%s,%s,%s,'selected')
                """,
                (siteid, canonical_url, baseline_id),
            )
            
        conn.commit()
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


# ============================================================
# BASELINE HASH HELPERS (IMMUTABLE)
# ============================================================

# Cache schema checks to avoid repeated queries
_SCHEMA_CACHE = {}
_SCHEMA_LOCK = threading.Lock()

def _has_column(table_name, column_name):
    """Check if a column exists in a table (cached per process)."""
    cache_key = f"{table_name}.{column_name}"
    
    with _SCHEMA_LOCK:
        if cache_key in _SCHEMA_CACHE:
            return _SCHEMA_CACHE[cache_key]
    
    # Not cached yet - check database
    conn = get_connection()
    try:
        cur = conn.cursor(buffered=True)
        cur.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            """,
            (table_name, column_name)
        )
        result = cur.fetchone()
        try:
             cur.fetchall()
        except:
             pass
        
        has_it = result is not None and result[0] > 0
        
        with _SCHEMA_LOCK:
            _SCHEMA_CACHE[cache_key] = has_it
        
        return has_it
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()

def upsert_baseline_hash(site_id, normalized_url, content_hash, baseline_path, baseline_id=None, base_url=None):
    """
    Insert or UPDATE baseline for a URL into defacement_sites.
    """
    canonical_url = LinkUtility.get_canonical_id(normalized_url, base_url)
    if not canonical_url:
        return False

    has_updated_at = _has_column('defacement_sites', 'updated_at')

    conn = get_connection()
    try:
        cur = conn.cursor(buffered=True)
        
        # Check if record exists
        cur.execute(
            "SELECT id FROM defacement_sites WHERE siteid=%s AND url=%s",
            (site_id, canonical_url)
        )
        row = cur.fetchone()
        try:
            cur.fetchall()
        except Exception:
            pass
        
        if row:
            if has_updated_at:
                cur.execute(
                    """
                    UPDATE defacement_sites
                    SET content_hash = %s,
                        baseline_path = %s,
                        baseline_id = %s,
                        action = 'selected',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (content_hash, baseline_path, baseline_id, row[0]),
                )
            else:
                cur.execute(
                    """
                    UPDATE defacement_sites
                    SET content_hash = %s,
                        baseline_path = %s,
                        baseline_id = %s,
                        action = 'selected'
                    WHERE id = %s
                    """,
                    (content_hash, baseline_path, baseline_id, row[0]),
                )
        else:
            # If it doesn't exist, we insert it with action='selected'
            cur.execute(
                """
                INSERT INTO defacement_sites
                    (siteid, url, content_hash, baseline_path, baseline_id, action)
                VALUES (%s, %s, %s, %s, %s, 'selected')
                """,
                (site_id, canonical_url, content_hash, baseline_path, baseline_id),
            )
            
        conn.commit()
        return True
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()

def fetch_baseline_hash(site_id, normalized_url, base_url=None):
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True, buffered=True)
        # Use canonical "Domain/Path" for lookup
        canonical_url = LinkUtility.get_canonical_id(normalized_url, base_url)
        cur.execute(
            """
            SELECT content_hash, baseline_path
            FROM defacement_sites
            WHERE siteid=%s AND url=%s AND content_hash IS NOT NULL
            """,
            (site_id, canonical_url),
        )
        row = cur.fetchone()
        try:
             cur.fetchall()
        except:
             pass
        return row
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


def site_has_baselines(site_id):
    conn = get_connection()
    try:
        cur = conn.cursor(buffered=True)
        cur.execute(
            "SELECT 1 FROM defacement_sites WHERE siteid=%s AND content_hash IS NOT NULL LIMIT 1",
            (site_id,),
        )
        row = cur.fetchone()
        try:
             cur.fetchall()
        except:
             pass
        return row is not None
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


def has_site_crawl_data(site_id, url=None):
    """Checks if crawl_pages already has entries for this site."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        if url:
            # Use canonical ID to match DB storage format
            canonical_url = LinkUtility.get_canonical_id(url)
            cur.execute(
                "SELECT 1 FROM crawl_pages WHERE siteid=%s AND url=%s LIMIT 1",
                (site_id, canonical_url),
            )
        else:
            cur.execute(
                "SELECT 1 FROM crawl_pages WHERE siteid=%s LIMIT 1",
                (site_id,),
            )
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


def fetch_site_info_by_baseline_id(baseline_id):
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT d.siteid, d.url, s.custid 
            FROM defacement_sites d
            JOIN sites s ON d.siteid = s.siteid
            WHERE d.baseline_id=%s
            """,
            (baseline_id,),
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
    canonical_url = LinkUtility.get_canonical_id(normalized_url, base_url)
    if not canonical_url:
        return

    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # 1. 🔍 Optimization: Check if current DB hash matches observed hash to avoid redundant write
        cur.execute(
            "SELECT observed_hash FROM observed_pages WHERE site_id=%s AND normalized_url=%s",
            (site_id, canonical_url)
        )
        row = cur.fetchone()
        
        if row and row[0] == observed_hash:
            # Hash unchanged - SKIP write
            # (If we wanted to update 'checked_at', we would do a small UPDATE here, but user asked to avoid inserts)
            return

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
        conn.commit()
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


def fetch_observed_page(site_id, normalized_url):
    """
    Fetch the last known observed state of a page.
    Used to skip re-processing if the hash hasn't changed.
    """
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT observed_hash, baseline_id, defacement_score, defacement_severity
            FROM observed_pages 
            WHERE site_id=%s AND normalized_url=%s
            """,
            (site_id, normalized_url),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()


def get_selected_defacement_rows():
    """Return defacement_sites rows marked as 'selected'.

    Uses the shared MySQL connection pool and DB_SEMAPHORE, so we must
    release the semaphore after closing the connection.
    """
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT siteid, url, baseline_id, threshold
            FROM defacement_sites
            WHERE action = 'selected'
            """
        )
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()

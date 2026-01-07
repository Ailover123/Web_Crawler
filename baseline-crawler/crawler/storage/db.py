"""
Database connection and initialization for the crawler.
Centralized MySQL database with connection pooling for multi-threaded crawling.
"""

import mysql.connector
from mysql.connector import pooling
from crawler.config import DB_CONFIG

# Create connection pool at module load time (singleton)
_db_pool = None

def _get_pool():
    """Get or create the connection pool."""
    global _db_pool
    if _db_pool is None:
        _db_pool = pooling.MySQLConnectionPool(
            pool_name="crawler_pool",
            pool_size=20,  # Reuse 20 connections
            pool_reset_session=True,
            **DB_CONFIG
        )
    return _db_pool

def get_connection():
    """
    Get a MySQL connection from the pool.
    Reuses existing connections instead of creating new ones (critical for performance).
    """
    pool = _get_pool()
    return pool.get_connection()

def initialize_db():
    """
    Initialize the centralized MySQL database with required tables.
    Only keep: sites, crawl_metrics, defacement_sites, defacement_details.
    This is idempotent - safe to call multiple times.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Sites table (centralized)
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS sites (
            siteid INT PRIMARY KEY AUTO_INCREMENT,
            url VARCHAR(255) UNIQUE NOT NULL,
            app_type VARCHAR(50),
            custid INT,
            added_by VARCHAR(100),
            time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_custid (custid),
            INDEX idx_url (url)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

        # Crawl metrics table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS crawl_metrics (
            id INT PRIMARY KEY AUTO_INCREMENT,
            url VARCHAR(255),
            fetch_status INT,
            speed_ms FLOAT,
            size_bytes INT,
            time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_url (url),
            INDEX idx_time (time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

        # Defacement monitoring sites
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS defacement_sites (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url VARCHAR(255) NOT NULL,
            group_id INT,
            email VARCHAR(300),
            email_cc1 VARCHAR(300),
            email_cc2 VARCHAR(300),
            action VARCHAR(20),
            siteid VARCHAR(100),
            threshold INT DEFAULT 1,
            changed_by INT,
            baseline_time TIMESTAMP NULL,
            defacement_monitor_status VARCHAR(255),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_def_sites_url (url(255))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

        # Defacement detection details
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS defacement_details (
            id INT AUTO_INCREMENT PRIMARY KEY,
            def_id INT,
            siteid VARCHAR(100),
            hash VARCHAR(300),
            baseline_hash VARCHAR(300),
            status VARCHAR(100),
            mail_sent VARCHAR(100),
            defacement VARCHAR(100),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            flagSetTime VARCHAR(50),
            KEY idx_defacement_details_siteid (siteid)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

        conn.commit()
        print("[DB] All tables initialized successfully")
    except mysql.connector.Error as err:
        if err.errno == 1050:  # Table already exists
            print("[DB] Tables already exist (idempotent)")
        else:
            print(f"[DB] Error initializing database: {err}")
            raise
    finally:
        cursor.close()
        conn.close()

def initialize_failed_db():
    """
    Failed DB is now integrated into centralized MySQL.
    This function is kept for compatibility but does nothing.
    """
    pass
 

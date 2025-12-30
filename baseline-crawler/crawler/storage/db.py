"""
Database connection and initialization for the crawler.
Creates the URLs table for storing crawl results.
"""

import sqlite3
from pathlib import Path
from crawler.config import DATA_DIR

DB_PATH = DATA_DIR / "crawler.db"

def get_connection():
    """
    Create and return a SQLite database connection.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def initialize_db():
    """
    Initialize the database tables.
    Drops and recreates the urls table for crawl results.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS urls;")
    cursor.execute("""
    CREATE TABLE urls (
        id INTEGER PRIMARY KEY,
        url TEXT UNIQUE NOT NULL,
        domain TEXT NOT NULL,
        status TEXT NOT NULL,  -- 'success', 'ignored', or 'fetch_failed'
        http_status INTEGER,   -- HTTP status code
        content_type TEXT,     -- Content-Type header
        response_size INTEGER, -- bytes
        fetch_time_ms INTEGER, -- milliseconds
        error_type TEXT,       -- 'http_error', 'timeout', 'connection_error', etc.
        discovered_from TEXT,  -- URL that led to this one, null for seeds
        depth INTEGER NOT NULL,-- crawl depth
        crawled_at TEXT NOT NULL  -- timestamp
    );
    """)
    conn.commit()
    conn.close()
 

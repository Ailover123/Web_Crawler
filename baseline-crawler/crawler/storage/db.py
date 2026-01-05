"""
Database connection and initialization for the crawler.
Creates domain-specific databases with the new schema.
"""

import sqlite3
from pathlib import Path
from crawler.config import DATA_DIR
import os

def get_db_path(domain):
    """
    Generate domain-specific database path.
    """
    return DATA_DIR / f"data_{domain}.db"

def get_connection(domain):
    """
    Create and return a SQLite database connection for a specific domain.
    """
    db_path = get_db_path(domain)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def get_failed_db_path(domain):
    """
    Generate domain-specific failed database path.
    """
    return DATA_DIR / f"failed_{domain}.db"

def get_failed_connection(domain):
    """
    Create and return a SQLite database connection for failed crawls of a specific domain.
    """
    db_path = get_failed_db_path(domain)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def initialize_db(domain):
    """
    Initialize the database tables for a specific domain.
    Creates the new schema table for crawl results.
    """
    conn = get_connection(domain)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS crawl_data;")
    cursor.execute("""
    CREATE TABLE crawl_data (
        domain TEXT,
        url TEXT PRIMARY KEY,
        routed_from TEXT,  -- The referrer URL
        urls_present_on_page TEXT,  -- JSON/Text list of outgoing links
        fetch_status INTEGER,  -- HTTP status code
        speed REAL,  -- Fetch duration in ms
        size INTEGER,  -- Response size in bytes
        timestamp TEXT  -- Time of crawl (ISO format)
    );
    """)
    conn.commit()
    conn.close()

def initialize_failed_db(domain):
    """
    Initialize the database tables for failed crawls of a specific domain.
    Creates the schema table for failed crawl results.
    """
    conn = get_failed_connection(domain)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS failed_crawl_data;")
    cursor.execute("""
    CREATE TABLE failed_crawl_data (
        domain TEXT,
        url TEXT PRIMARY KEY,
        routed_from TEXT,  -- The referrer URL
        fetch_status INTEGER,  -- HTTP status code
        error_message TEXT,  -- Error message
        timestamp TEXT  -- Time of crawl (ISO format)
    );
    """)
    conn.commit()
    conn.close()
 

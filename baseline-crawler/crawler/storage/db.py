#connection + table creation
# Centralized SQLite database initialization.
# Responsibilities:
# - create database connection
# - initialize required tables
# - provide a shared connection for storage modules

import sqlite3
from pathlib import Path

DB_PATH = Path("data/crawler.db")

def get_connection():
    #Create and return a SQLite database connection.
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    # Enable foreign key constraints for data integrity
    return conn

def initialize_db():
  # Initialize database tables if they do not exist.
  conn = get_connection()
  cursor = conn.cursor()
  # Cursor is used to execute SQL commands
  cursor.execute("""
  CREATE TABLE IF NOT EXISTS urls (
      id INTEGER PRIMARY KEY,
      url TEXT UNIQUE,
      first_discovered_at TEXT,
      last_crawled_at TEXT,
      crawl_depth INTEGER,
      status INTEGER           
  );
  """)
  # URL inventory table tracks crawl coverage and scope

  cursor.execute(""" 
  CREATE TABLE IF NOT EXISTS baseline (
    id INTEGER PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    html_hash TEXT NOT NULL,
    script_sources TEXT,
    baseline_created_at TEXT,
    baseline_updated_at TEXT
  );
  """)
  # Baseline table stores trusted fingerprints for defacement detection

  conn.commit()
  cursor.close()
  conn.close()  
# Commit schema changes and close connection
 

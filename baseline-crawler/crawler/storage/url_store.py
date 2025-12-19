# #URL Inventory
# Data access layer for the URL inventory table (`urls`).

# Responsibilities:
# - insert newly discovered URLs
# - update crawl metadata
# - query existing URLs

# This module isolates raw SQL from crawler logic.

from datetime import datetime, timezone
from crawler.storage.db import get_connection

def now():
  return datetime.now(timezone.utc).isoformat()

def insert_url(url, crawl_depth):
# Insert a new URL into the inventory.
 conn = get_connection()
 cursor = conn.cursor()
 cursor.execute("""
 INSERT OR IGNORE INTO urls 
 (url, first_discovered_at, crawl_depth, status)
 VALUES (?, ?, ?, ?);
 """, 
 (url, now(), crawl_depth, "active")
 )
 # INSERT OR IGNORE prevents duplicate URL entries
 conn.commit()
 cursor.close()
 conn.close()
 

def update_crawl_metadata(url, status):
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute("""
  UPDATE urls
  Set last_crawled_at = ?, status = ?
  WHERE url = ?
  """,
  (now(), status, url)
  )
# Update crawl timestamp and status for the given URL
  conn.commit()
  cursor.close()
  conn.close()
# Status reflects crawl outcome, not defacement state

def url_exists(url):
  #Check if a URL exists in the inventory.
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute("""
  SELECT 1 FROM urls WHERE url = ? LIMIT 1;
  """, 
  (url,)) 

  result = cursor.fetchone()
  cursor.close()
  conn.close()
  return result is not None
# Returns True if URL exists, else False

def get_active_urls():
  #Retrieve all active URLs for crawling.
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute("""
  SELECT url, crawl_depth FROM urls WHERE status = 'active';
  """)
  
  rows = cursor.fetchall()
  cursor.close()
  conn.close()             

  return rows
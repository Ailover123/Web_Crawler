# #Baseline Storage
# Responsibilities:
# - persist trusted baseline fingerprints per URL
# - retrieve baseline data for comparison
# - update baselines only during baseline creation phase

import json
from datetime import datetime, timezone
from crawler.storage.db import get_connection
from crawler.normalizer import normalize_url

def now():
  return datetime.now(timezone.utc).isoformat()

def store_baseline(url, html_hash, script_sources, script_count=None):
  #Insert or update baseline for a URL.
  # Normalize URL key so lookups are consistent
  nurl = normalize_url(url)
  # Persist script_sources and script_count together so callers can pass script_count
  payload = {
    'sources': script_sources or [],
    'count': script_count or (len(script_sources) if script_sources else 0)
  }
  scripts_json = json.dumps(payload)
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute("""
  INSERT INTO baseline 
  (url, html_hash, script_sources, baseline_created_at, baseline_updated_at)
  VALUES (?, ?, ?, ?, ?)
  ON CONFLICT(url) DO UPDATE SET
  html_hash=excluded.html_hash,
  script_sources=excluded.script_sources,
  baseline_updated_at=excluded.baseline_updated_at;
  """,
  (nurl, html_hash, scripts_json, now(), now())
  )
  conn.commit()
  cursor.close()
  conn.close()


def get_baseline(url):
  #Retrieve baseline data for a URL.
  conn = get_connection()
  cursor = conn.cursor()
  nurl = normalize_url(url)
  cursor.execute("""
  SELECT html_hash, script_sources FROM baseline WHERE url = ?""",
  (nurl,)
  )

  row = cursor.fetchone()
  if row is None:
    cursor.close()
    conn.close()
    return None
  
  else:
    html_hash, scripts_json = row
    parsed = json.loads(scripts_json) if scripts_json else {'sources': [], 'count': 0}
    script_sources = parsed.get('sources', [])
    script_count = parsed.get('count', 0)
    cursor.close()
    conn.close()
    return {
        "html_hash": html_hash,
        "script_sources": script_sources,
        "script_count": script_count
    }

def baseline_exists(url):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM baseline WHERE url = ? LIMIT 1",
        (normalize_url(url),)
    )
    exists = cur.fetchone() is not None
    conn.close()
    return exists

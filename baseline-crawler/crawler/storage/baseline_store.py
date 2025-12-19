# #Baseline Storage
# Responsibilities:
# - persist trusted baseline fingerprints per URL
# - retrieve baseline data for comparison
# - update baselines only during baseline creation phase

import json
from datetime import datetime, timezone
from crawler.storage.db import get_connection

def now():
  return datetime.now(timezone.utc).isoformat()

def store_baseline(url, html_hash, script_sources):
  #Insert or update baseline for a URL.

  scripts_json = json.dumps(script_sources)
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
  (url, html_hash, scripts_json, now(), now())
  )
  conn.commit()
  cursor.close()
  conn.close()


def get_baseline(url):
  #Retrieve baseline data for a URL.
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute("""
  SeLECT html_hash, script_sources FROM baseline WHERE url = ?""",
  (url,)
  )

  row = cursor.fetchone()
  if row is None:
    cursor.close()
    conn.close()
    return None
  
  else:
    html_hash, scripts_json = row
    script_sources = json.loads(scripts_json) if scripts_json else []
    cursor.close()
    conn.close()
    return {
      "html_hash": html_hash,
      "script_sources": script_sources
    }
# #Diff for Proof of Change Detection
# Responsibilities:
# - store immutable diff records after detection
# - support forensic review and audit
# - provide proof of defacement to users

import json
from datetime import datetime, timezone
from crawler.storage.db import get_connection

def now():
  return datetime.now(timezone.utc).isoformat()

def store_diff(url,baseline_hash,observed_hash, diff_summary, severity = "medium", status = "open"):
  #Insert a new diff evidence record.
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute("""
  INSERT INTO diff_evidence
  (url, baseline_hash, observed_hash, diff_summary, severity, detected_at, status)
  VALUES (?, ?, ?, ?, ?, ?, ?);
  """,
  (url, baseline_hash, observed_hash, json.dumps(diff_summary), severity, now(), status)
  )
  conn.commit()
  cursor.close()
  conn.close()  

def get_open_diffs():
  #Retireve open diff records for a URL.
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute("""
    SELECT baseline_hash, observed_hash, diff_summary, severity, detected_at
    FROM diff_evidence
    WHERE status = 'open'
    ORDER BY detected_at DESC;
    """
  )
  rows = cursor.fetchall()
  cursor.close()
  conn.close()

  return rows


#!/usr/bin/env python3
"""Drop all crawler tables and reinitialize the schema (clean reset)."""
import sys
sys.path.insert(0, '.')

from crawler.storage.db import initialize_db, get_connection

KEEP_TABLES = {"sites", "crawl_metrics", "defacement_sites", "defacement_details"}

def drop_unwanted_tables():
	conn = get_connection()
	cur = conn.cursor()
	try:
		print("[DB] Dropping unwanted tables...")
		cur.execute("SET FOREIGN_KEY_CHECKS=0")
		cur.execute("SHOW TABLES")
		existing = [row[0] for row in cur.fetchall()]
		to_drop = [t for t in existing if t not in KEEP_TABLES]
		for t in to_drop:
			cur.execute(f"DROP TABLE IF EXISTS {t}")
		cur.execute("SET FOREIGN_KEY_CHECKS=1")
		conn.commit()
		print(f"[DB] Dropped: {', '.join(to_drop) if to_drop else 'none'}")
	finally:
		cur.close()
		conn.close()

if __name__ == "__main__":
	print("Resetting database to contain only required tables...")
	drop_unwanted_tables()
	initialize_db()
	print("✓ Database reset. Keeping only: sites, crawl_metrics, defacement_sites, defacement_details")

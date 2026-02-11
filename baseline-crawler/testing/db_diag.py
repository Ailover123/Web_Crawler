import sys
import os
import logging

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from crawler.storage.mysql import get_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import os
from dotenv import load_dotenv

load_dotenv()

print(f"DEBUG: Host={os.getenv('MYSQL_HOST')}")
print(f"DEBUG: Port={os.getenv('MYSQL_PORT')}")
print(f"DEBUG: User={os.getenv('MYSQL_USER')}")
print(f"DEBUG: DB={os.getenv('MYSQL_DATABASE')}")

def run_diagnostics():
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        
        # 1. Check Sites Schema
        print("\n--- SITES_OLD TABLE COUNT ---")
        try:
            cur.execute("SELECT COUNT(*) as total FROM sites_old")
            print(f"Total Sites Old: {cur.fetchone()['total']}")
        except Exception as e:
            print(f"sites_old error: {e}")

        print("\n--- CRAWL_JOBS FOREIGN KEYS ---")
        cur.execute("""
            SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = 'crawlerdb' AND TABLE_NAME = 'crawl_jobs' AND REFERENCED_TABLE_NAME IS NOT NULL
        """)
        for row in cur.fetchall():
            print(row)

        # 2. Check Defacement Sites
        print("\n--- DEFACEMENT_SITES TABLE ---")
        cur.execute("SELECT COUNT(*) as total FROM defacement_sites")
        total_ds = cur.fetchone()['total']
        print(f"Total Defacement Sites entries: {total_ds}")
        
        # Check specific recent sites
        for siteid in [9520, 2200]:
            cur.execute(f"SELECT COUNT(*) as count FROM defacement_sites WHERE siteid={siteid}")
            count = cur.fetchone()['count']
            print(f"Entries for Site {siteid}: {count}")
            
            if count > 0:
                cur.execute(f"SELECT * FROM defacement_sites WHERE siteid={siteid} LIMIT 5")
                print(f"Sample data for {siteid}:")
                for row in cur.fetchall():
                    print(row)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    run_diagnostics()

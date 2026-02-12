import os
import sys

# Ensure we can import crawler modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from crawler.storage.mysql import get_connection
except ImportError:
    print("Error: Could not import crawler.storage.mysql. Make sure you are running this from the root 'baseline-crawler' folder.")
    sys.exit(1)

def inspect_server():
    print("\n=== DEBUGGING DEV SERVER DB ===\n")
    
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        
        # 1. Connection Info
        cur.execute("SELECT DATABASE(), USER(), VERSION()")
        info = cur.fetchone()
        print(f"Connected to: {info}")
        
        # 2. Check Tables
        print("\n--- Checking Tables ---")
        cur.execute("SHOW TABLES")
        tables = [list(r.values())[0] for r in cur.fetchall()]
        has_defacement = 'defacement_sites' in tables
        has_crawl_defacement = 'crawl_defacement_sites' in tables
        
        print(f"Table 'defacement_sites' exists: {has_defacement}")
        print(f"Table 'crawl_defacement_sites' exists: {has_crawl_defacement}")
        
        # 3. Inspect 'crawl_defacement_sites' (if exists)
        if has_crawl_defacement:
            print("\n--- Inspecting 'crawl_defacement_sites' ---")
            cur.execute("SELECT COUNT(*) as cnt FROM crawl_defacement_sites")
            count = cur.fetchone()['cnt']
            print(f"Total Rows: {count}")
            
            # Check for 'selected' action
            cur.execute("SELECT COUNT(*) as cnt FROM crawl_defacement_sites WHERE action='selected'")
            selected = cur.fetchone()['cnt']
            print(f"Rows with action='selected': {selected}")
            
            # Sample Rows
            print("First 5 'selected' rows:")
            cur.execute("SELECT siteid, url, action FROM crawl_defacement_sites WHERE action='selected' LIMIT 5")
            for row in cur.fetchall():
                print(row)
        else:
            print("\n[!] 'crawl_defacement_sites' TABLE NOT FOUND.")

        # 4. Inspect 'defacement_sites' (comparison)
        if has_defacement:
            print("\n--- Inspecting 'defacement_sites' ---")
            cur.execute("SELECT COUNT(*) as cnt FROM defacement_sites")
            count = cur.fetchone()['cnt']
            print(f"Total Rows: {count}")
            
            cur.execute("SELECT COUNT(*) as cnt FROM defacement_sites WHERE action='selected'")
            selected = cur.fetchone()['cnt']
            print(f"Rows with action='selected': {selected}")

        cur.close()
        conn.close()
        print("\n=== END DEBUG ===")
        
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")

if __name__ == "__main__":
    inspect_server()

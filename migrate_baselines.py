import os
import shutil
import mysql.connector
from dotenv import load_dotenv

# --- CONFIGURATION ---
TARGET_VALUE = "migrate_baseline"  # The name of the main folder to be created
OLD_BASELINE_PATH = "/home/priti/Web-Crawler/Web_Crawler/old-baselines"
NEW_BASELINE_PARENT = "/home/priti/Web-Crawler/Web_Crawler"  # Parent directory where TARGET_VALUE will be created

OLD_TABLE = "defacement_sites_dev"
NEW_TABLE = "defacement_sites_migrate"

def migrate_baselines():
    """
    Refers to old and new defacement tables to map URLs to IDs,
    recreates the sub-folder structure, and copies/renames baseline files.
    """
    load_dotenv()
    
    # Database connection parameters from .env
    db_config = {
        'host': os.getenv('MYSQL_HOST', '127.0.0.1'),
        'user': os.getenv('MYSQL_USER', 'root'),
        'password': os.getenv('MYSQL_PASSWORD', ''),
        'database': os.getenv('MYSQL_DATABASE', 'crawlerdb')
    }
    
    print("Connecting to database...")
    try:
        conn = mysql.connector.connect(**db_config)
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return

    cursor = conn.cursor(dictionary=True)
    
    try:
        # 1. Fetch old table mappings (siteid -> URL)
        print(f"Fetching mappings from {OLD_TABLE}...")
        cursor.execute(f"SELECT url, siteid FROM {OLD_TABLE}")
        old_rows = cursor.fetchall()
        id_to_url = {row['siteid']: row['url'] for row in old_rows if row['siteid']}
        
        # 2. Fetch new table mappings (URL -> baseline_id)
        print(f"Fetching mappings from {NEW_TABLE}...")
        cursor.execute(f"SELECT url, baseline_id FROM {NEW_TABLE}")
        new_rows = cursor.fetchall()
        url_to_new_id = {row['url']: row['baseline_id'] for row in new_rows if row['baseline_id']}

        # 3. Setup Target Directory
        target_dir = os.path.join(NEW_BASELINE_PARENT, TARGET_VALUE)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
            print(f"Created main target folder: {target_dir}")
        else:
            print(f"Using existing target folder: {target_dir}")

        # 4. Recursive Walk through Old Baselines
        print(f"Starting recursive walk through: {OLD_BASELINE_PATH}")
        total_copied = 0
        total_skipped = 0

        for root, dirs, files in os.walk(OLD_BASELINE_PATH):
            for filename in files:
                if not filename.endswith('.html'):
                    continue
                
                # Extract the old ID from the filename (e.g., '93299-1.html' -> '93299-1')
                old_id = filename.rsplit('.', 1)[0]
                
                # Map old ID to URL
                url = id_to_url.get(old_id)
                if not url:
                    print(f"  [Skipped] No URL mapping found for old ID: {old_id}")
                    total_skipped += 1
                    continue
                
                # Map URL to New Baseline ID
                new_id = url_to_new_id.get(url)
                if not new_id:
                    print(f"  [Skipped] No mapping to new baseline_id for URL: {url}")
                    total_skipped += 1
                    continue
                
                # Recreate the relative path structure
                rel_path = os.path.relpath(root, OLD_BASELINE_PATH)
                dest_root = os.path.join(target_dir, rel_path)
                
                if not os.path.exists(dest_root):
                    os.makedirs(dest_root)
                
                src_file = os.path.join(root, filename)
                dest_file = os.path.join(dest_root, f"{new_id}.html")
                
                # Copy and rename
                try:
                    shutil.copy2(src_file, dest_file)
                    print(f"  [Copied] {old_id} -> {new_id} ({url[:50]}...)")
                    total_copied += 1
                except Exception as e:
                    print(f"  [Error] Failed to copy {src_file}: {e}")

        print("\nMigration Summary:")
        print(f"  Total files successfully copied: {total_copied}")
        print(f"  Total files skipped: {total_skipped}")
        print(f"  Migration folder located at: {target_dir}")

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    migrate_baselines()

import os
import shutil
import mysql.connector
import logging
from datetime import datetime
from dotenv import load_dotenv

# --- CONFIGURATION ---
TARGET_VALUE = "migrate_baseline"  # The name of the main folder to be created
OLD_BASELINE_PATH = "/home/priti/Web-Crawler/Web_Crawler/old-baselines"
NEW_BASELINE_PARENT = "/home/priti/Web-Crawler/Web_Crawler"  # Parent directory where TARGET_VALUE will be created

OLD_TABLE = "defacement_sites_dev"
NEW_TABLE = "defacement_sites_migrate"

def setup_logging():
    """Sets up logging to both console and a file in the baseline-crawler/logs structure."""
    base_log_dir = os.path.join(NEW_BASELINE_PARENT, "baseline-crawler", "logs")
    
    # Create date-based subfolder
    date_folder = datetime.now().strftime("%Y-%m-%d")
    log_dir = os.path.join(base_log_dir, date_folder)
    
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Filename format: MIGRATE_HHMMSS.log
    timestamp = datetime.now().strftime("%H%M%S")
    log_file = os.path.join(log_dir, f"MIGRATE_{timestamp}.log")
    
    # Clear existing logging configuration to ensure fresh setup
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return log_file

def migrate_baselines():
    """
    Refers to old and new defacement tables to map URLs to IDs,
    recreates the sub-folder structure, and copies/renames baseline files.
    """
    load_dotenv()
    log_file = setup_logging()
    logging.info(f"Starting migration. Logs are being saved to: {log_file}")
    
    # Database connection parameters from .env
    db_config = {
        'host': os.getenv('MYSQL_HOST', '127.0.0.1'),
        'user': os.getenv('MYSQL_USER', 'root'),
        'password': os.getenv('MYSQL_PASSWORD', ''),
        'database': os.getenv('MYSQL_DATABASE', 'crawlerdb')
    }
    
    logging.info("Connecting to database...")
    try:
        conn = mysql.connector.connect(**db_config)
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to MySQL: {err}")
        return

    cursor = conn.cursor(dictionary=True)
    
    try:
        # 1. Fetch old table mappings
        logging.info(f"Fetching mappings from {OLD_TABLE}...")
        cursor.execute(f"SELECT id, url, siteid FROM {OLD_TABLE}")
        old_rows = cursor.fetchall()
        
        # Two types of old mappings:
        # a) siteid -> URL (for standard baselines like '93299.html')
        siteid_to_url = {row['siteid']: row['url'] for row in old_rows if row['siteid']}
        # b) id (int) -> URL (for files mapped in defacement_files)
        id_to_url = {row['id']: row['url'] for row in old_rows if row['id']}
        
        # 1.5. Fetch fallback mappings from defacement_files (for backups)
        FILES_TABLE = "defacement_files"
        logging.info(f"Fetching fallback mappings from {FILES_TABLE}...")
        cursor.execute(f"SELECT id, baseline FROM {FILES_TABLE} WHERE baseline IS NOT NULL")
        file_rows = cursor.fetchall()
        # baseline_filename -> site_id
        baseline_to_site_id = {row['baseline']: row['id'] for row in file_rows if row['baseline']}

        # 2. Fetch new table mappings (URL -> baseline_id)
        logging.info(f"Fetching mappings from {NEW_TABLE}...")
        cursor.execute(f"SELECT url, baseline_id FROM {NEW_TABLE}")
        new_rows = cursor.fetchall()
        url_to_new_id = {row['url']: row['baseline_id'] for row in new_rows if row['baseline_id']}

        # 3. Setup Target Directory
        target_dir = os.path.join(NEW_BASELINE_PARENT, TARGET_VALUE)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
            logging.info(f"Created main target folder: {target_dir}")
        else:
            logging.info(f"Using existing target folder: {target_dir}")

        # 4. Recursive Walk through Old Baselines
        logging.info(f"Starting recursive walk through: {OLD_BASELINE_PATH}")
        total_copied = 0
        total_skipped = 0

        for root, dirs, files in os.walk(OLD_BASELINE_PATH):
            is_backup_dir = "backups" in os.path.split(root)[1] or "backups" in root.split(os.sep)
            
            for filename in files:
                if not filename.endswith('.html'):
                    continue
                
                url = None
                map_source = ""

                # Attempt A: Standard siteid mapping (e.g., '93299-1.html' -> '93299-1')
                old_id = filename.rsplit('.', 1)[0]
                url = siteid_to_url.get(old_id)
                if url:
                    map_source = OLD_TABLE
                
                # Attempt B: Fallback Files mapping (e.g., backup files like '32363-1770978159.html')
                if not url:
                    site_id = baseline_to_site_id.get(filename)
                    if site_id:
                        url = id_to_url.get(site_id)
                        if url:
                            map_source = FILES_TABLE

                if not url:
                    logging.warning(f"  [Skipped] No URL mapping found for {filename} in {OLD_TABLE} or {FILES_TABLE}")
                    total_skipped += 1
                    continue
                
                # Map URL to New Baseline ID
                new_id = url_to_new_id.get(url)
                if not new_id:
                    logging.warning(f"  [Skipped] No mapping to new baseline_id for URL: {url} in {NEW_TABLE}")
                    total_skipped += 1
                    continue
                
                # Recreate the relative path structure
                rel_path = os.path.relpath(root, OLD_BASELINE_PATH)
                dest_root = os.path.join(target_dir, rel_path)
                
                if not os.path.exists(dest_root):
                    os.makedirs(dest_root)
                
                src_file = os.path.join(root, filename)
                
                # Naming Logic: Preserve original name if it's a backup, otherwise use new_id
                if is_backup_dir:
                    dest_file = os.path.join(dest_root, filename)
                    rename_msg = f"{filename} (preserved backup name)"
                else:
                    dest_file = os.path.join(dest_root, f"{new_id}.html")
                    rename_msg = f"{filename} -> {new_id}.html"
                
                # Copy and rename
                try:
                    shutil.copy2(src_file, dest_file)
                    logging.info(f"  [Copied] {rename_msg} (via {map_source})")
                    total_copied += 1
                except Exception as e:
                    logging.error(f"  [Error] Failed to copy {src_file}: {e}")

        logging.info("\nMigration Summary:")
        logging.info(f"  Total files successfully copied: {total_copied}")
        logging.info(f"  Total files skipped: {total_skipped}")
        logging.info(f"  Migration folder located at: {target_dir}")
        logging.info(f"  Log file saved at: {log_file}")

    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    migrate_baselines()

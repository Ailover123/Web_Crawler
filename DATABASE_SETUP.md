# Database Setup Instructions

This guide explains how to initialize the crawler database schema using phpMyAdmin (XAMPP).

## Prerequisites

- XAMPP installed and running
- MySQL service started
- Database `crawlerdb` already exists
- Legacy tables `sites` and `crawled_urls` may already exist (they will NOT be modified)

## Step-by-Step Instructions

### 1. Open phpMyAdmin

1. Start XAMPP Control Panel
2. Ensure MySQL is running (green indicator)
3. Click "Admin" button next to MySQL, or navigate to: `http://localhost/phpmyadmin`

### 2. Select Database

1. In the left sidebar, click on `crawlerdb`
2. If `crawlerdb` doesn't exist, create it:
   - Click "New" in the left sidebar
   - Enter database name: `crawlerdb`
   - Choose collation: `utf8mb4_unicode_ci`
   - Click "Create"

### 3. Import Schema

1. Click the **"Import"** tab at the top
2. Click **"Choose File"** button
3. Navigate to your project directory and select:
   ```
   D:\academic\Web_Crawler\database\init.sql
   ```
4. Leave all other settings as default
5. Scroll down and click **"Go"** button

### 4. Verify Success

You should see a success message like:
```
Import has been successfully finished, 7 queries executed.
```

Click on the database name in the left sidebar to see all tables. You should now have:

**Legacy Tables (preserved):**
- `sites`
- `crawled_urls`

**New Internal Tables:**
- `crawl_sessions`
- `task_store`
- `crawl_artifacts`
- `rendered_artifacts`
- `site_baselines`
- `baseline_profiles`
- `detection_verdicts`

## Important Notes

### Idempotency
The `init.sql` script is **safe to run multiple times**. It uses `CREATE TABLE IF NOT EXISTS`, so:
- Existing tables will NOT be dropped
- Existing data will NOT be deleted
- Only missing tables will be created

### Legacy Data Protection
The script will **NEVER**:
- Drop the `sites` table
- Drop the `crawled_urls` table
- Modify existing columns
- Delete any data

### Re-running the Script
If you need to reset only the internal tables (keeping legacy data):

1. In phpMyAdmin, select `crawlerdb`
2. Click "SQL" tab
3. Run this query to drop internal tables:
   ```sql
   DROP TABLE IF EXISTS detection_verdicts;
   DROP TABLE IF EXISTS baseline_profiles;
   DROP TABLE IF EXISTS site_baselines;
   DROP TABLE IF EXISTS rendered_artifacts;
   DROP TABLE IF EXISTS crawl_artifacts;
   DROP TABLE IF EXISTS task_store;
   DROP TABLE IF EXISTS crawl_sessions;
   ```
4. Then re-import `init.sql` as described above

## Troubleshooting

### Error: "Table already exists"
This is normal and harmless. The script uses `IF NOT EXISTS` to skip existing tables.

### Error: "Cannot add foreign key constraint"
This means the parent table doesn't exist. Make sure you're running the complete `init.sql` file, not partial queries.

### Error: "Access denied"
Check your MySQL credentials in `baseline-crawler/crawler/config.py`:
```python
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # Update if you set a password
    'database': 'crawlerdb',
    ...
}
```

## Next Steps

After successful schema initialization:

1. Verify you have at least one active site:
   ```sql
   SELECT * FROM sites WHERE is_active = 1;
   ```

2. Run the baseline crawler:
   ```bash
   python run_with_logs.py --mode baseline
   ```

3. Check the logs in `logs/crawl_session_<timestamp>.txt`

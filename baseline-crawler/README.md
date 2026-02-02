# 🕸️ Enterprise Web Crawler & Defacement Detection

A high-performance, dynamic-scaling web crawler with JavaScript rendering capabilities, designed for site seeding, baseline generation, and semantic defacement detection.

---

## 🏗️ Architecture "The Scene"
The system is designed to handle **Hybrid Enterprise Stacks** (WordPress + Laravel + SPAs). It uses:
*   **Playwright**: For full JS rendering (React/Vue/Angular).
*   **Semantic Hashing**: Stable content fingerprinting that ignores timestamps and noisy metadata.
*   **Dynamic Scaling**: Automatically adjusts worker counts (100+ URLs queue pressure triggers scale-up).
*   **MySQL Pooling**: Optimized for high-concurrency parallel site processing.

---

## 🚀 Getting Started

### 1. Installation
```bash
# Activate your environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
playwright install chromium
```

### 2. Configuration (.env)
Create a `.env` in the root directory:
```bash
# Database settings
MYSQL_HOST=localhost
MYSQL_USER=your_user
MYSQL_PASSWORD=your_pass
MYSQL_DATABASE=crawlerdb
MYSQL_POOL_SIZE=32

# Scaling settings
MIN_WORKERS=5
MAX_WORKERS=50
MAX_PARALLEL_SITES=3

# Operation Mode (CRAWL | BASELINE | COMPARE)
CRAWL_MODE=CRAWL
```

---

## ⚙️ Operational Modes

| Mode | Purpose |
| :--- | :--- |
| **`CRAWL`** | Discovery mode. Finds new URLs and seeds the `crawl_pages` table. |
| **`BASELINE`** | "Snapshot" mode. Downloads current live HTML and saves to `baselines/` folder. |
| **`COMPARE`** | Detection mode. Compares live content against the "Semantic Hash" of the baseline. |

---

## 🛠️ Usage Examples

### Standard Site Crawl (Discovery)
`python3 main.py --log`

### High-Performance Parallel Run (Multi-site)
`python3 main.py --parallel --max_parallel_sites 3 --log`

### Debug One Specific Site
`python3 main.py --siteid 10102 --log`

### Real-time Log-to-File Run
`python3 run_and_log.py --parallel --log`

---

## 🔍 Debugging & Maintenance

We have built-in tools in the `debug/` folder:
1.  **Analyze DB Updates**: `python3 debug/parse_updates.py logs/YOUR_LOG.log`
    *   *Generates a .txt report showing exactly which URLs triggered updates.*
2.  **Enable All Sites**: `python3 debug/enable_all_sites.py`
    *   *Quickly resets all sites in the DB to `enabled=1`.*

---

## 📁 Project Structure
- `main.py`: Main orchestration & dynamic scaling engine.
- `crawler/worker.py`: The individual worker thread logic.
- `crawler/js_renderer.py`: Playwright JS execution.
- `crawler/storage/`: Database and file system sync.
- `debug/`: Utility scripts for maintenance.
- `logs/`: Session logs and IST summaries.

---

## 🛡️ Best Practices
*   **MySQL Pool**: Never set `MYSQL_POOL_SIZE` above 32 (Library limit).
*   **Crawl Delay**: Keep `CRAWL_DELAY` at `1.0` in `config.py` to avoid getting blocked by LiteSpeed/WAFs.
*   **Cleanup**: Regularly check the `logs/` folder as it grows with every session.

 #!/usr/bin/env python3
"""
Unified Streamlit UI for baseline crawler.
Features: List domains, show analysis (charts, stats), compare runs (select old run, show new/missing URLs), display timestamps in IST.
"""

import streamlit as st
import pandas as pd
import json
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from crawler.config import DATA_DIR

def load_analysis_data(domain):
    """
    Load JSON analysis data for a specific domain.
    """
    json_file = DATA_DIR / f"{domain}_analysis.json"
    if json_file.exists():
        with open(json_file, 'r') as f:
            return json.load(f)
    return None

def get_available_domains():
    """
    Get list of available domains from analysis JSON files.
    """
    domains = []
    for json_file in DATA_DIR.glob("*_analysis.json"):
        domain = json_file.stem.replace("_analysis", "")
        domains.append(domain)
    return sorted(domains)

def get_old_runs(domain):
    """
    Get list of old runs for a domain from data/old_runs/.
    """
    old_runs_dir = DATA_DIR / "old_runs"
    if not old_runs_dir.exists():
        return []
    old_runs = []
    for db_file in old_runs_dir.glob(f"data_{domain}_*.db"):
        timestamp_str = db_file.stem.replace(f"data_{domain}_", "")
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
            old_runs.append((timestamp, db_file))
        except ValueError:
            continue
    return sorted(old_runs, key=lambda x: x[0], reverse=True)

def load_old_analysis(domain, timestamp):
    """
    Load analysis for an old run by generating it from the old DB.
    """
    from analysis_generator import generate_analysis_for_domain
    old_db_path = DATA_DIR / "old_runs" / f"data_{domain}_{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}.db"
    st.info(f"Loading old analysis from: {old_db_path}")
    if old_db_path.exists():
        try:
            analysis = generate_analysis_for_domain(domain, old_db_path)
            st.info(f"Loaded {analysis['total_urls']} URLs from old DB")
            return analysis
        except Exception as e:
            st.error(f"Error loading old analysis: {e}")
            return None
    else:
        st.error(f"Old DB file not found: {old_db_path}")
        return None

def utc_to_ist(utc_str):
    """
    Convert UTC timestamp to IST.
    """
    try:
        utc_dt = datetime.fromisoformat(utc_str.replace('Z', '+00:00'))
        ist_dt = utc_dt + timedelta(hours=5, minutes=30)
        return ist_dt.strftime("%Y-%m-%d %H:%M:%S IST")
    except:
        return utc_str

def main():
    st.title("Baseline Crawler Analysis Dashboard")

    # Sidebar: Domain selector
    st.sidebar.header("Domain Selector")
    domains = get_available_domains()
    if not domains:
        st.error("No analysis data found. Run analysis_generator.py first.")
        return

    selected_domain = st.sidebar.selectbox("Select Domain", domains)

    # Load current analysis data
    data = load_analysis_data(selected_domain)
    if not data:
        st.error(f"No analysis data found for domain {selected_domain}")
        return

    # Display current analysis
    st.header(f"Analysis for {selected_domain}")
    st.metric("Total URLs", data["total_urls"])

    # Classifications chart
    classifications = {k: v["count"] for k, v in data["distribution"].items()}
    df_class = pd.DataFrame(list(classifications.items()), columns=["Type", "Count"])
    st.bar_chart(df_class.set_index("Type"))

    # URLs by type
    st.subheader("URLs by Type")
    for typ, info in data["distribution"].items():
        with st.expander(f"{typ} ({info['count']} URLs)"):
            urls_df = pd.DataFrame(info["urls"])
            st.dataframe(urls_df)

    # Run Comparison
    st.header("Run Comparison")
    old_runs = get_old_runs(selected_domain)
    if not old_runs:
        st.warning("No old runs found for comparison.")
        return

    # Select old run
    run_options = [f"{ts.strftime('%Y-%m-%d %H:%M:%S')} (IST)" for ts, _ in old_runs]
    selected_run = st.selectbox("Select Old Run for Comparison", run_options)

    if selected_run:
        # Find selected old run
        selected_ts_str = selected_run.split(" (IST)")[0]
        selected_ts = datetime.strptime(selected_ts_str, "%Y-%m-%d %H:%M:%S")

        # Load old analysis
        old_data = load_old_analysis(selected_domain, selected_ts)
        if old_data:
            st.subheader(f"Comparing Current Run vs {selected_run}")

            # Summary metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Current Total URLs", data["total_urls"])
            with col2:
                st.metric("Old Total URLs", old_data["total_urls"])
            with col3:
                diff = data["total_urls"] - old_data["total_urls"]
                st.metric("Difference", f"{diff:+d}")

            # Two-column comparison layout
            col1, col2 = st.columns(2)

            with col1:
                st.subheader(f"Current Run ({datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')})")
                with st.container(height=400):
                    # Classifications chart
                    classifications = {k: v["count"] for k, v in data["distribution"].items()}
                    df_class = pd.DataFrame(list(classifications.items()), columns=["Type", "Count"])
                    st.bar_chart(df_class.set_index("Type"))

                    # URLs by type
                    for typ, info in data["distribution"].items():
                        with st.expander(f"{typ} ({info['count']} URLs)", expanded=False):
                            urls_df = pd.DataFrame(info["urls"])
                            st.dataframe(urls_df, height=200)

            with col2:
                st.subheader(f"Old Run ({selected_run})")
                with st.container(height=400):
                    # Classifications chart
                    old_classifications = {k: v["count"] for k, v in old_data["distribution"].items()}
                    df_old_class = pd.DataFrame(list(old_classifications.items()), columns=["Type", "Count"])
                    st.bar_chart(df_old_class.set_index("Type"))

                    # URLs by type
                    for typ, info in old_data["distribution"].items():
                        with st.expander(f"{typ} ({info['count']} URLs)", expanded=False):
                            urls_df = pd.DataFrame(info["urls"])
                            st.dataframe(urls_df, height=200)

            # New/Missing URLs analysis
            st.subheader("URL Changes")
            current_urls = set()
            for typ, info in data["distribution"].items():
                for url_info in info["urls"]:
                    current_urls.add(url_info["url"])

            old_urls = set()
            for typ, info in old_data["distribution"].items():
                for url_info in info["urls"]:
                    old_urls.add(url_info["url"])

            new_urls = current_urls - old_urls
            missing_urls = old_urls - current_urls

            col1, col2 = st.columns(2)
            with col1:
                st.metric("New URLs", len(new_urls))
                if new_urls:
                    with st.expander("View New URLs"):
                        st.write(list(new_urls)[:50])  # Show first 50
                        if len(new_urls) > 50:
                            st.write(f"... and {len(new_urls) - 50} more")

            with col2:
                st.metric("Missing URLs", len(missing_urls))
                if missing_urls:
                    with st.expander("View Missing URLs"):
                        st.write(list(missing_urls)[:50])  # Show first 50
                        if len(missing_urls) > 50:
                            st.write(f"... and {len(missing_urls) - 50} more")
        else:
            st.error("Could not load old run data.")

if __name__ == "__main__":
    main()

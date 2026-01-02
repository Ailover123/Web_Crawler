#!/usr/bin/env python3
"""
DEPRECATED: This Streamlit UI is deprecated. Use ui.py instead.

Modern web-based dashboard using Streamlit for visualizing crawl data.
Features: Domain selector, data grid, and iteration comparison.
"""

import streamlit as st
import pandas as pd
import json
import os
from pathlib import Path
from crawler.config import DATA_DIR

def load_domain_data(domain):
    """
    Load JSON data for a specific domain.
    """
    json_file = DATA_DIR / f"export_{domain}.json"
    if json_file.exists():
        with open(json_file, 'r') as f:
            return json.load(f)
    return []

def get_available_domains():
    """
    Get list of available domains from JSON files.
    """
    domains = []
    for json_file in DATA_DIR.glob("export_*.json"):
        domain = json_file.stem.replace("export_", "")
        domains.append(domain)
    return sorted(domains)

def main():
    st.title("Web Crawler Data Visualization Dashboard")

    # Sidebar: Domain selector
    st.sidebar.header("Domain Selector")
    domains = get_available_domains()
    if not domains:
        st.error("No domain data found. Run export_data.py first.")
        return

    selected_domain = st.sidebar.selectbox("Select Domain", domains)

    # Load data
    data = load_domain_data(selected_domain)
    if not data:
        st.error(f"No data found for domain {selected_domain}")
        return

    df = pd.DataFrame(data)

    # Data Grid
    st.header(f"Data Grid for {selected_domain}")
    st.dataframe(df[['url', 'speed', 'size', 'fetch_status']])

    # Iteration Comparison
    st.header("Iteration Comparison")
    timestamps = df['timestamp'].unique()
    if len(timestamps) < 2:
        st.warning("Need at least two different timestamps for comparison.")
        return

    col1, col2 = st.columns(2)
    with col1:
        old_timestamp = st.selectbox("Old Run", timestamps)
    with col2:
        new_timestamp = st.selectbox("New Run", timestamps, index=1 if len(timestamps) > 1 else 0)

    if old_timestamp and new_timestamp:
        old_urls = set(df[df['timestamp'] == old_timestamp]['url'])
        new_urls = set(df[df['timestamp'] == new_timestamp]['url'])

        new_files = new_urls - old_urls
        missing_files = old_urls - new_urls

        st.subheader("New Files (in new run, not in old)")
        st.write(list(new_files))

        st.subheader("Missing Files (in old run, not in new)")
        st.write(list(missing_files))

if __name__ == "__main__":
    main()

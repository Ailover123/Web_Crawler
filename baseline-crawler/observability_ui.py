# File: observability_ui.py
# Purpose: Provide read-only UI for crawl observability
# Phase: Post-crawl analysis
# Output: Flask web app for viewing reports
# Notes: Reads from JSON files only, no crawling or DB writes

from flask import Flask, render_template, request, jsonify
import json
import os

import os
app = Flask(__name__, template_folder=os.path.join(os.getcwd(), 'ui', 'templates'))

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/domains')
def get_domains():
    if os.path.exists('combined_domain_analysis.json'):
        with open('combined_domain_analysis.json', 'r') as f:
            data = json.load(f)
        # data is now a dict with "domains" key containing domain data
        if "domains" in data:
            domains = list(data["domains"].keys())
            print(f"observability_ui: returning {len(domains)} domains from combined_domain_analysis.json")
            return jsonify(domains)
    # Fallback: try to extract domains from routing_graph.json
    if os.path.exists('routing_graph.json'):
        try:
            with open('routing_graph.json', 'r') as f:
                rg = json.load(f)
            # routing_graph keys are normalized URLs; extract netlocs
            from urllib.parse import urlparse
            domains = set()
            for u in rg.keys():
                try:
                    domains.add(urlparse(u).netloc)
                except Exception:
                    continue
            domains_list = sorted(list(domains))
            print(f"observability_ui: fallback returning {len(domains_list)} domains from routing_graph.json")
            return jsonify(domains_list)
        except Exception:
            pass
    return jsonify([])

@app.route('/api/domain/<domain>')
def get_domain_data(domain):
    if os.path.exists('combined_domain_analysis.json'):
        with open('combined_domain_analysis.json', 'r') as f:
            data = json.load(f)
        # data is now a dict with "domains" key containing domain data
        if "domains" in data and domain in data["domains"]:
            return jsonify(data["domains"][domain])
    return jsonify({})

@app.route('/api/routing_graph')
def get_routing_graph():
    if os.path.exists('routing_graph.json'):
        with open('routing_graph.json', 'r') as f:
            data = json.load(f)
        return jsonify(data)
    return jsonify({})

if __name__ == '__main__':
    app.run(debug=True)

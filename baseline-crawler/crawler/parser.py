from bs4 import BeautifulSoup
from urllib.parse import urljoin

def parse_html(html, base_url):
    """
    Parse HTML content and extract:
    - soup (BeautifulSoup object)
    - links (absolute URLs from <a href>)
    - script_sources (external <script src>)
    - script_count (inline + external scripts)
    """
    soup = BeautifulSoup(html, 'html.parser')

    links = []
    script_sources = []

    # Extract links
    for a in soup.find_all('a', href=True):
        absolute_url = urljoin(base_url, a['href'])
        links.append(absolute_url)

    # Extract scripts
    all_scripts = soup.find_all('script')
    script_count = len(all_scripts)

    for script in all_scripts:
        if script.has_attr('src'):
            absolute_url = urljoin(base_url, script['src'])
            script_sources.append(absolute_url)

    return soup, links, script_sources, script_count

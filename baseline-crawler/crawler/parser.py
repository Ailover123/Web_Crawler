# To parse HTML, JS, JSON baseline data into structured format
 
from bs4 import BeautifulSoup
from urllib.parse import urljoin  
# urljoin is used to safely resolve relative URLs like "/about"

# Inputs:
# - html: raw HTML string from HTTP response
# - base_url: URL of the page (used for resolving relative links)

# Returns:
# - soup: BeautifulSoup object (HTML structure)
# - links: list of absolute URLs found in <a href>
# - scripts: list of absolute URLs found in <script src>

def parse_html(html, base_url):
    """Parse HTML content and extract links and text."""
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    scripts=[]
 
    for a in soup.find_all('a', href=True):
        absolute_url = urljoin(base_url, a['href'])
        links.append(absolute_url)
#Here we are using urljoin to convert relative URLs to absolute URLs based on the base_url of the page.
# This ensures that links like "/about" are correctly resolved to "http://example.com/about" if the base_url is "http://example.com/page".
# This is crucial for accurate link extraction in web crawling.

    for script in soup.find_all('script', src=True):
        absolute_url = urljoin(base_url, script['src'])  
        scripts.append(absolute_url)
# Similarly, resolving script src URLs to absolute URLs             
    
    return soup,links,scripts
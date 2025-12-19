#File to make HTTP requests to target URLs and DNS + TLS handshakes + HTTP response + certificate validation via requests library

import requests
from crawler.config import USER_AGENT, REQUEST_TIMEOUT

#Input needed is URL
#Output is HTTP Response or None in case of failure 
def fetch(url):
  #Fetches URL via HTTP GET request
  try:
    response = requests.get(
      url, 
      timeout=REQUEST_TIMEOUT,
      headers={
        "User-Agent": USER_AGENT},
        verify=True 
   )
    response.raise_for_status()  # Raise error for HTTP error responses (4xx, 5xx)
    return response
  except requests.exceptions.RequestException:      
     # Any network, DNS, TLS, or connection error is treated as a fetch failure
    return None
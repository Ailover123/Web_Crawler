#To remove needless noise from baseline data.
# Input: parsed HTML structure
# Process:
#   - normalize tags
#   - normalize attributes
#   - clean whitespace
# Output: stable string

def normalize_html(soup):
  for tag in soup.find_all(True):
      tag.name = tag.name.lower()
# HTML tag names are case-insensitive; normalize to lowercase
      if tag.attrs:
         #Normalise class lists
         if "class" in tag.attrs:
          tag.attrs["class"] = sorted(tag.attrs["class"])
          #Sort attributes
         tag.attrs = dict(sorted(tag.attrs.items()))
# Sort attributes to ensure consistent ordering so that string comparison are reliable
  return str(soup)


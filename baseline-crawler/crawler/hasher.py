#To hash incoming baseline data
# Input: normalized string
# Output: hash value

import hashlib
def hash_content(normalized_content):
  sha = hashlib.sha256()
  sha.update(normalized_content.encode("utf-8"))
  # Convert string to bytes before hashing
  return sha.hexdigest()
  # Return hexadecimal representation of the hash

  
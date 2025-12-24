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

def hash_json_keys(obj):
    def collect(o, prefix=""):
        out = []
        if isinstance(o, dict):
            for k in sorted(o):
                path = f"{prefix}/{k}"
                out.append(path)
                out.extend(collect(o[k], path))
        elif isinstance(o, list):
            for v in o:
                out.extend(collect(v, prefix + "/*"))
        return out

    keys = collect(obj)
    return hash_content("\n".join(keys))

  
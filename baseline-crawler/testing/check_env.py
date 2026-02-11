import os
import mysql.connector
from dotenv import load_dotenv

# Force reload of .env
load_dotenv(override=True)

print("--- ENV VARS ---")
host = os.getenv("MYSQL_HOST")
port = os.getenv("MYSQL_PORT")
user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
database = os.getenv("MYSQL_DATABASE")

print(f"HOST: '{host}'")
print(f"PORT: '{port}'")
print(f"USER: '{user}'")
print(f"PASS: '{password}'")
print(f"DB:   '{database}'")

print("\n--- ATTEMPTING CONNECTION ---")
try:
    cnx = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    print("✅ Connection SUCCESS!")
    cnx.close()
except Exception as e:
    print(f"❌ Connection FAILED: {e}")

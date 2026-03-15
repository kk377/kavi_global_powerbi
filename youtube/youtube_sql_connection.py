import os
import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

# === Azure SQL credentials (set in .env) ===
SERVER = os.getenv("AZURE_SQL_SERVER", "your-server.database.windows.net")
DB     = os.getenv("AZURE_SQL_DB", "your-database")
USER   = os.getenv("AZURE_SQL_USER", "your-username")
PWD    = os.getenv("AZURE_SQL_PWD")
DRIVER = os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

if not PWD:
    raise RuntimeError("Missing AZURE_SQL_PWD in environment. Check your .env file.")

# === Connection string ===
conn_str = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DB};"
    f"UID={USER};"
    f"PWD={PWD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

engine = create_engine(
    "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_str),
    fast_executemany=True,
)

# === Load CSVs ===
df_channel = pd.read_csv("kaviglobal_channel_snapshot.csv")
df_videos  = pd.read_csv("kaviglobal_recent_videos.csv")

# === Write to staging tables (auto-creates them) ===
df_channel.to_sql(
    "youtube_channel_snapshot",
    engine,
    schema="stg",
    if_exists="replace",
    index=False
)

df_videos.to_sql(
    "youtube_recent_videos",
    engine,
    schema="stg",
    if_exists="replace",
    index=False
)

print("✅ Loaded:")
print("stg.youtube_channel_snapshot")
print("stg.youtube_recent_videos")

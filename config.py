import os
import json
import logging
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import threading

# --- Logging Config ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# --- Env & App Config ---
load_dotenv()
CLIENT_NAME = os.getenv("CLIENT_NAME", "DefaultUser")
TCP_SERVER_PORT = int(os.getenv("TCP_SERVER_PORT", 5001))
FLASK_WEB_PORT = int(os.getenv("FLASK_WEB_PORT", 8001))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 3))
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", 30))
MAX_CLIENTS = int(os.getenv("MAX_CLIENTS", 10))
BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", 50))

# User timezone
USER_TIMEZONE = os.getenv("USER_TIMEZONE", "Asia/Kolkata")

# Peer list
try:
    with open("peers.json", "r") as f:
        PEER_LIST = json.load(f).get("peers", [])
except FileNotFoundError:
    PEER_LIST = []
    logging.warning("[Config] peers.json not found, running without peers")

# Database config
DB_DRIVER = os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}")
DB_SERVER = os.getenv("DB_SERVER", "localhost\\SQLEXPRESS")
DB_NAME = os.getenv("DB_NAME", "chatdb")
DB_TRUSTED = os.getenv("DB_TRUSTED", "yes")

db_connection_str = (
    f"DRIVER={DB_DRIVER};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_NAME};"
    f"Trusted_Connection={DB_TRUSTED};"
)
params = urllib.parse.quote_plus(db_connection_str)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", echo=False)
Session = sessionmaker(bind=engine)

# Globals
client_sockets = []
client_semaphore = threading.Semaphore(MAX_CLIENTS)

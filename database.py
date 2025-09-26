from datetime import datetime
import logging
from sqlalchemy import text
from config import engine, USER_TIMEZONE
from utils import convert_utc_to_local

def initialize_database():
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='messages' AND xtype='U')
                CREATE TABLE messages (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    sender VARCHAR(255) NOT NULL,
                    timestamp DATETIME2 NOT NULL,
                    message TEXT NOT NULL
                )
            """))
            conn.execute(text("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ledger' AND xtype='U')
                CREATE TABLE ledger (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    sender VARCHAR(255) NOT NULL,
                    timestamp DATETIME2 NOT NULL,
                    message TEXT NOT NULL,
                    prev_hash VARCHAR(64) NOT NULL,
                    hash VARCHAR(64) NOT NULL UNIQUE
                )
            """))
            conn.commit()
            logging.info("[DB] Tables initialized successfully")
    except Exception as e:
        logging.error(f"[DB Error] Failed to initialize database: {e}")

def get_last_block_hash():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT TOP 1 hash FROM ledger ORDER BY id DESC"))
            row = result.fetchone()
            return row[0] if row else "0"
    except:
        return "0"

def get_ledger_count():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) as count FROM ledger"))
            return result.fetchone()[0]
    except:
        return 0

def get_ledger_blocks(start_index=0, limit=100):
    try:
        start_index = max(0, start_index)
        limit = max(1, limit)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT sender, timestamp, message, prev_hash, hash
                FROM ledger
                ORDER BY timestamp ASC, id ASC
                OFFSET :start_index ROWS
                FETCH NEXT :limit ROWS ONLY
            """), {"start_index": start_index, "limit": limit}).mappings().all()
            blocks = []
            for row in result:
                ts = row["timestamp"]
                utc_ts = ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(ts, datetime) else ts
                blocks.append({
                    "sender": row["sender"],
                    "timestamp": utc_ts,
                    "display_timestamp": convert_utc_to_local(utc_ts),
                    "message": row["message"],
                    "prev_hash": row["prev_hash"],
                    "hash": row["hash"]
                })
            return blocks
    except Exception as e:
        logging.error(f"[DB Error] get_ledger_blocks: {e}")
        return []

def get_ledger_blocks_before(before_timestamp, limit=20):
    try:
        limit = max(1, limit)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT sender, timestamp, message, prev_hash, hash
                FROM ledger
                WHERE timestamp < :before_timestamp
                ORDER BY timestamp DESC, id DESC
                OFFSET 0 ROWS
                FETCH NEXT :limit ROWS ONLY
            """), {"before_timestamp": before_timestamp, "limit": limit}).mappings().all()
            blocks = []
            for row in result:
                ts = row["timestamp"]
                utc_ts = ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(ts, datetime) else ts
                blocks.append({
                    "sender": row["sender"],
                    "timestamp": utc_ts,
                    "display_timestamp": convert_utc_to_local(utc_ts),
                    "message": row["message"],
                    "prev_hash": row["prev_hash"],
                    "hash": row["hash"]
                })
            return list(reversed(blocks))
    except Exception as e:
        logging.error(f"[DB Error] get_ledger_blocks_before: {e}")
        return []

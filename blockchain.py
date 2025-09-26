import hashlib
import json
import logging
from datetime import datetime
from sqlalchemy import text
from config import engine
from utils import convert_utc_to_local

def calculate_hash(sender, timestamp, message, prev_hash=""):
    return hashlib.sha256(f"{sender}{timestamp}{message}{prev_hash}".encode()).hexdigest()

def handle_new_block(block_json, safe_emit):
    try:
        block = json.loads(block_json)
        calc_hash = calculate_hash(block["sender"], str(block["timestamp"]), block["message"], block["prev_hash"])
        if calc_hash == block["hash"]:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM ledger WHERE hash = :h"), {"h": block["hash"]})
                if result.fetchone()[0] == 0:
                    conn.execute(
                        text("INSERT INTO messages (sender, timestamp, message) VALUES (:sender, :timestamp, :message)"),
                        {"sender": block["sender"], "timestamp": block["timestamp"], "message": block["message"]}
                    )
                    conn.execute(
                        text("INSERT INTO ledger (sender, timestamp, message, prev_hash, hash) VALUES (:sender, :timestamp, :message, :prev_hash, :hash)"),
                        block
                    )
                    conn.commit()
            block_with_display = block.copy()
            block_with_display["display_timestamp"] = convert_utc_to_local(block["timestamp"])
            safe_emit("receive_message", block_with_display, to_all=True)
            logging.info(f"[New Block] Received and broadcasted block from {block['sender']}")
    except Exception as e:
        logging.error(f"[Blockchain Error] {e}")

def broadcast_block_to_peers(block_data, client_sockets):
    message = f"NEW_BLOCK:{json.dumps(block_data, default=str)}\n"
    for sock in client_sockets[:]:
        try:
            sock.send(message.encode())
        except:
            client_sockets.remove(sock)
            sock.close()

def validate_chain():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT id, sender, timestamp, message, prev_hash, hash FROM ledger ORDER BY id ASC")).mappings().all()
            prev_hash = "0"
            for b in result:
                timestamp_str = b["timestamp"].strftime("%Y-%m-%d %H:%M:%S") if isinstance(b["timestamp"], datetime) else str(b["timestamp"])
                if b["prev_hash"] != prev_hash or calculate_hash(b["sender"], timestamp_str, b["message"], b["prev_hash"]) != b["hash"]:
                    return False
                prev_hash = b["hash"]
        return True
    except:
        return False

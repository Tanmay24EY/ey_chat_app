import socket
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from flask import Flask, render_template_string
from flask_socketio import SocketIO
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
import json
import hashlib
import logging
 
# --- Logging Config ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
 
# --- App & Config ---
app = Flask(__name__)
socketio = SocketIO(app, logger=False, engineio_logger=False)  # suppress socketio logs
 
load_dotenv()
CLIENT_NAME = os.getenv("CLIENT_NAME", "DefaultUser")
TCP_SERVER_PORT = int(os.getenv("TCP_SERVER_PORT", 5001))
FLASK_WEB_PORT = int(os.getenv("FLASK_WEB_PORT", 8001))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 3))
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", 30))
MAX_CLIENTS = int(os.getenv("MAX_CLIENTS", 10))
BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", 50))
 
# Expect peers.json like [{"ip":"127.0.0.1","port":5002}, ...]
with open("peers.json", "r") as f:
    PEER_LIST = json.load(f).get("peers", [])
 
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
 
# --- Globals ---
client_sockets = []
client_semaphore = threading.Semaphore(MAX_CLIENTS)
 
# --- Utility: Safe emit ---
def safe_emit(event, data, **kwargs):
    try:
        payload = json.loads(json.dumps(data, default=str))
        socketio.emit(event, payload, **kwargs)
    except Exception as e:
        logging.error(f"[Emit Error] Failed to emit {event}: {e}")
 
# --- Blockchain Helpers ---
def calculate_hash(sender, timestamp, message, prev_hash=""):
    return hashlib.sha256(f"{sender}{timestamp}{message}{prev_hash}".encode()).hexdigest()
 
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
        # Ensure start_index is never negative
        start_index = max(0, start_index)
        # Ensure limit is positive
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
                if isinstance(ts, datetime):
                    ts = ts.isoformat()
                blocks.append({
                    "sender": row["sender"],
                    "timestamp": ts,
                    "message": row["message"],
                    "prev_hash": row["prev_hash"],
                    "hash": row["hash"]
                })
            return blocks
    except Exception as e:
        logging.error(f"[DB Error] get_ledger_blocks: {e}")
        return []
 
# --- Peer Sync Functions ---
def request_ledger_sync():
    local_count = get_ledger_count()
 
    # Handle empty ledger case
    if local_count == 0:
        last_hash = "0"
        last_prev_hash = "0"
    else:
        # Get the last block (use max(0, local_count - 1) to ensure non-negative index)
        last_block = get_ledger_blocks(max(0, local_count - 1), 1)
        last_hash = last_block[0]["hash"] if last_block else "0"
        last_prev_hash = last_block[0]["prev_hash"] if last_block else "0"
 
    payload = {"last_hash": last_hash, "last_prev_hash": last_prev_hash, "total_count": local_count}
    logging.info(f"[Sync] Sending SYNC_REQUEST to peers with last_hash={last_hash} total_count={local_count}")
    for sock in client_sockets[:]:
        try:
            sock.send(f"SYNC_REQUEST:{json.dumps(payload)}\n".encode())
        except:
            client_sockets.remove(sock)
            sock.close()
 
def handle_sync_request(conn, payload_json):
    try:
        data = json.loads(payload_json)
        peer_last_hash = data["last_hash"]
        peer_last_prev = data["last_prev_hash"]
        peer_count = data["total_count"]
        local_count = get_ledger_count()
 
        if local_count == peer_count and peer_last_hash == get_last_block_hash():
            response = {"blocks": [], "total_count": local_count}
            conn.send(f"SYNC_RESPONSE:{json.dumps(response)}\n".encode())
            logging.info(f"[Sync] Peer already up-to-date. Sent empty response.")
            return
 
        missing_blocks = []
        with engine.connect() as conn_db:
            result = conn_db.execute(text("""
                SELECT sender, timestamp, message, prev_hash, hash
                FROM ledger
                ORDER BY id ASC
            """)).mappings().all()
 
            start_index = 0
            for i, b in enumerate(result):
                if b["hash"] == peer_last_hash and b["prev_hash"] == peer_last_prev:
                    start_index = i + 1
                    break
 
            prev_hash = peer_last_hash
            # Ensure we don't go beyond the result length
            end_index = min(len(result), start_index + BATCH_SIZE)
 
            for b in result[start_index:end_index]:
                if b["prev_hash"] != prev_hash:
                    break
                ts = b["timestamp"]
                if isinstance(ts, datetime):
                    ts = ts.isoformat()
                missing_blocks.append({
                    "sender": b["sender"],
                    "timestamp": ts,
                    "message": b["message"],
                    "prev_hash": b["prev_hash"],
                    "hash": b["hash"]
                })
                prev_hash = b["hash"]
 
        response_data = {"blocks": missing_blocks, "total_count": local_count}
        conn.send(f"SYNC_RESPONSE:{json.dumps(response_data)}\n".encode())
        logging.info(f"[Sync] Sent {len(missing_blocks)} missing blocks to peer.")
    except Exception as e:
        logging.error(f"[Sync Request Error] {e}")
 
def handle_sync_response(response_json):
    try:
        data = json.loads(response_json)
        peer_blocks = data.get("blocks", [])
        if not peer_blocks:
            logging.info(f"[Sync] No missing blocks from peer. Chain is up-to-date.")
            safe_emit("sync_status", {"status": "synced"})
            safe_emit("chat_history", get_ledger_blocks(0, 50))
            return
 
        blocks_added = 0
        with engine.connect() as conn:
            last_local_hash = get_last_block_hash()
            for block in peer_blocks:
                exists = conn.execute(
                    text("SELECT COUNT(*) FROM ledger WHERE hash = :h"),
                    {"h": block["hash"]}
                ).fetchone()[0]
                if exists == 0:
                    conn.execute(
                        text("INSERT INTO messages (sender, timestamp, message) VALUES (:sender, :timestamp, :message)"),
                        {"sender": block["sender"], "timestamp": block["timestamp"], "message": block["message"]}
                    )
                    conn.execute(
                        text("INSERT INTO ledger (sender, timestamp, message, prev_hash, hash) VALUES (:sender, :timestamp, :message, :prev_hash, :hash)"),
                        block
                    )
                    conn.commit()
                    blocks_added += 1
                    last_local_hash = block["hash"]
        logging.info(f"[Sync] Added {blocks_added} new blocks from peer batch.")
 
        safe_emit("sync_status", {"status": "synced"})
        safe_emit("chat_history", get_ledger_blocks(0, 50))
 
        if len(peer_blocks) == BATCH_SIZE:
            logging.info(f"[Sync] Full batch received ({BATCH_SIZE}). Requesting next batch...")
            request_ledger_sync()
    except Exception as e:
        logging.error(f"[Sync Response Error] {e}")
        safe_emit("sync_status", {"status": "error"})
 
# --- TCP Server ---
def handle_client(conn, addr):
    buffer = ""
    try:
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            buffer += data
            while "\n" in buffer:
                msg, buffer = buffer.split("\n", 1)
                if msg.startswith("NEW_BLOCK:"):
                    handle_new_block(msg[10:])
                elif msg.startswith("SYNC_REQUEST:"):
                    handle_sync_request(conn, msg[13:])
                elif msg.startswith("SYNC_RESPONSE:"):
                    handle_sync_response(msg[14:])
                else:
                    handle_regular_message(msg)
    except Exception as e:
        logging.error(f"[TCP Error] {e}")
    finally:
        conn.close()
 
def handle_new_block(block_json):
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
            safe_emit("receive_message", block)
            safe_emit("chat_history", get_ledger_blocks(0, 50))
    except Exception as e:
        logging.error(f"[Blockchain Error] {e}")
 
def handle_regular_message(message):
    safe_emit("receive_message", {"sender": "peer", "message": message, "timestamp": datetime.now().isoformat()})
 
def start_tcp_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", TCP_SERVER_PORT))
    server.listen(5)
    logging.info(f"[TCP] Server running on {TCP_SERVER_PORT}")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
 
# --- Peer Listener Thread ---
def listen_to_peer(sock, ip, port):
    buffer = ""
    try:
        while True:
            data = sock.recv(1024).decode()
            if not data:
                break
            buffer += data
            while "\n" in buffer:
                msg, buffer = buffer.split("\n", 1)
                if msg.startswith("NEW_BLOCK:"):
                    handle_new_block(msg[10:])
                elif msg.startswith("SYNC_REQUEST:"):
                    handle_sync_request(sock, msg[13:])
                elif msg.startswith("SYNC_RESPONSE:"):
                    handle_sync_response(msg[14:])
                else:
                    handle_regular_message(msg)
    except Exception as e:
        logging.error(f"[Peer Listen Error] {ip}:{port} - {e}")
    finally:
        if sock in client_sockets:
            client_sockets.remove(sock)
        sock.close()
        logging.info(f"[Peer] Disconnected: {ip}:{port}")
 
def connect_to_peers():
    for peer in PEER_LIST:
        ip = peer.get("ip")
        port = peer.get("port")
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((ip, port))
                client_sockets.append(s)
                logging.info(f"[TCP] Connected to peer {ip}:{port}")
                threading.Thread(target=listen_to_peer, args=(s, ip, port), daemon=True).start()
                request_ledger_sync()
                break
            except Exception as e:
                logging.warning(f"[TCP] Connection attempt {attempt} to {ip}:{port} failed: {e}")
                time.sleep(RETRY_DELAY)
 
def validate_chain():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT id, sender, timestamp, message, prev_hash, hash FROM ledger ORDER BY id ASC")).mappings().all()
            prev_hash = "0"
            for b in result:
                if b["prev_hash"] != prev_hash or calculate_hash(b["sender"], str(b["timestamp"]), b["message"], b["prev_hash"]) != b["hash"]:
                    return False
                prev_hash = b["hash"]
        return True
    except:
        return False
 
def periodic_ledger_sync():
    while True:
        time.sleep(SYNC_INTERVAL)
        logging.info("[Sync] Checking chain validity...")
        if not validate_chain():
            logging.warning("[Sync] Local chain invalid, requesting sync")
            request_ledger_sync()
 
# --- Web UI ---
CHAT_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Blockchain Chat</title>
    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
</head>
<body>
    <h2>Welcome, {{ client_name }}</h2>
    <div id="chat" style="border:1px solid #ccc; height:300px; overflow-y:scroll;"></div>
    <input id="message" type="text" placeholder="Type your message...">
    <button onclick="sendMessage()">Send</button>
 
    <script>
        var socket = io();
 
        socket.on("receive_message", function(data) {
            var chat = document.getElementById("chat");
            chat.innerHTML += "<p><b>" + data.sender + ":</b> " + data.message + " <i>(" + data.timestamp + ")</i></p>";
            chat.scrollTop = chat.scrollHeight;
        });
 
        socket.on("chat_history", function(messages) {
            var chat = document.getElementById("chat");
            chat.innerHTML = "";
            messages.forEach(function(m) {
                chat.innerHTML += "<p><b>" + m.sender + ":</b> " + m.message + " <i>(" + m.timestamp + ")</i></p>";
            });
        });
 
        socket.on("sync_status", function(data) {
            console.log("Sync Status:", data.status);
        });
 
        function sendMessage() {
            var msg = document.getElementById("message").value;
            socket.emit("send_message", msg);
            document.getElementById("message").value = "";
        }
 
        socket.emit("refresh_chat");
    </script>
</body>
</html>
"""
 
@app.route("/")
def index():
    return render_template_string(CHAT_TEMPLATE, client_name=CLIENT_NAME)
 
@socketio.on("send_message")
def handle_send_message(msg):
    timestamp = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    prev_hash = get_last_block_hash()
    block_hash = calculate_hash(CLIENT_NAME, timestamp, msg, prev_hash)
    block = {"sender": CLIENT_NAME, "timestamp": timestamp, "message": msg, "prev_hash": prev_hash, "hash": block_hash}
    try:
        with engine.connect() as conn:
            conn.execute(text("INSERT INTO messages (sender, timestamp, message) VALUES (:sender, :timestamp, :message)"),
                         {"sender": CLIENT_NAME, "timestamp": timestamp, "message": msg})
            conn.execute(text("INSERT INTO ledger (sender, timestamp, message, prev_hash, hash) VALUES (:sender, :timestamp, :message, :prev_hash, :hash)"),
                         block)
            conn.commit()
    except Exception as e:
        logging.error(f"[DB Error] {e}")
    safe_emit("receive_message", block)
    safe_emit("chat_history", get_ledger_blocks(0, 50))
    broadcast_block_to_peers(block)
 
@socketio.on("refresh_chat")
def handle_refresh_chat(data=None):
    safe_emit("chat_history", get_ledger_blocks(0, 50))
 
def broadcast_block_to_peers(block_data):
    message = f"NEW_BLOCK:{json.dumps(block_data, default=str)}\n"
    for sock in client_sockets[:]:
        try:
            sock.send(message.encode())
        except:
            client_sockets.remove(sock)
            sock.close()
 
# --- Main Run ---
if __name__ == "__main__":
    initialize_database()
    if not validate_chain():
        logging.warning("[Startup] Local chain invalid. Sync may be needed.")
    threading.Thread(target=start_tcp_server, daemon=True).start()
    threading.Thread(target=connect_to_peers, daemon=True).start()
    threading.Thread(target=periodic_ledger_sync, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=FLASK_WEB_PORT)
 
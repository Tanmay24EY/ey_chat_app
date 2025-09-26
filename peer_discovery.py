import socket
import threading
import time
import logging
import json
from datetime import datetime
from sqlalchemy import text
from blockchain import validate_chain, handle_new_block
from utils import safe_emit, get_utc_timestamp, convert_utc_to_local
from config import TCP_SERVER_PORT, PEER_LIST, MAX_RETRIES, RETRY_DELAY, SYNC_INTERVAL, client_semaphore, client_sockets
from database import get_ledger_count, get_ledger_blocks, get_last_block_hash, engine

BATCH_SIZE = 50  # number of blocks to sync per batch

# ------------------------ Client Handler ------------------------ #
def handle_client(conn, addr):
    acquired = client_semaphore.acquire(blocking=False)
    if not acquired:
        logging.warning(f"[TCP] Connection from {addr} rejected (max clients reached)")
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except:
            pass
        conn.close()
        return

    buffer = ""
    try:
        while True:
            try:
                data = conn.recv(1024).decode()
                if not data:
                    break
                buffer += data
                while "\n" in buffer:
                    msg, buffer = buffer.split("\n", 1)
                    if msg.startswith("NEW_BLOCK:"):
                        handle_new_block(msg[10:], safe_emit)
                    elif msg.startswith("SYNC_REQUEST:"):
                        handle_sync_request(conn, msg[13:])
                    elif msg.startswith("SYNC_RESPONSE:"):
                        handle_sync_response(msg[14:])
                    else:
                        safe_emit("receive_message", {
                            "sender": "peer",
                            "message": msg,
                            "timestamp": get_utc_timestamp(),
                            "display_timestamp": convert_utc_to_local(get_utc_timestamp())
                        }, to_all=True)
            except (ConnectionResetError, ConnectionAbortedError) as e:
                logging.warning(f"[TCP] Connection lost with {addr}: {e}")
                break
    except Exception as e:
        logging.error(f"[TCP Error] {e}")
    finally:
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except:
            pass
        conn.close()
        client_semaphore.release()
        logging.info(f"[TCP] Client disconnected: {addr}")

# ------------------------ Peer Listener ------------------------ #
def listen_to_peer(sock, ip, port):
    buffer = ""
    try:
        while True:
            try:
                data = sock.recv(1024).decode()  # blocking recv
                if not data:
                    break
                buffer += data
                while "\n" in buffer:
                    msg, buffer = buffer.split("\n", 1)
                    if msg.startswith("NEW_BLOCK:"):
                        handle_new_block(msg[10:], safe_emit)
                    elif msg.startswith("SYNC_REQUEST:"):
                        handle_sync_request(sock, msg[13:])
                    elif msg.startswith("SYNC_RESPONSE:"):
                        handle_sync_response(msg[14:])
                    else:
                        safe_emit("receive_message", {
                            "sender": "peer",
                            "message": msg,
                            "timestamp": get_utc_timestamp(),
                            "display_timestamp": convert_utc_to_local(get_utc_timestamp())
                        }, to_all=True)
            except (ConnectionResetError, ConnectionAbortedError) as e:
                logging.warning(f"[Peer Listen Warning] Connection closed by peer {ip}:{port} - {e}")
                break
    except Exception as e:
        logging.error(f"[Peer Listen Error] {ip}:{port} - {e}")
    finally:
        if sock in client_sockets:
            client_sockets.remove(sock)
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except:
            pass
        sock.close()
        client_semaphore.release()
        logging.info(f"[Peer] Disconnected: {ip}:{port}")

# ------------------------ TCP Server ------------------------ #
def start_tcp_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", TCP_SERVER_PORT))
    server.listen(5)
    logging.info(f"[TCP] Server running on {TCP_SERVER_PORT}")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

# ------------------------ Peer Connector ------------------------ #
def connect_to_peers():
    for peer in PEER_LIST:
        ip = peer.get("ip")
        port = peer.get("port")
        for attempt in range(1, MAX_RETRIES + 1):
            s = None
            acquired = client_semaphore.acquire(blocking=False)
            if not acquired:
                logging.warning(f"[Peer] Max clients reached. Skipping connection to {ip}:{port}")
                break
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # no timeout, blocking socket
                s.connect((ip, port))
                client_sockets.append(s)
                logging.info(f"[TCP] Connected to peer {ip}:{port}")
                threading.Thread(target=listen_to_peer, args=(s, ip, port), daemon=True).start()
                request_ledger_sync()
                break
            except (ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError) as e:
                logging.warning(f"[TCP] Connection attempt {attempt} to {ip}:{port} failed: {e}")
                time.sleep(RETRY_DELAY)
                if s:
                    try:
                        s.shutdown(socket.SHUT_RDWR)
                    except:
                        pass
                    s.close()
                if attempt == MAX_RETRIES:
                    logging.warning(f"[TCP] Max retries reached for {ip}:{port}. Releasing semaphore.")
                    client_semaphore.release()
            except Exception as e:
                logging.error(f"[TCP] Unexpected error connecting to {ip}:{port} - {e}")
                if s:
                    try:
                        s.shutdown(socket.SHUT_RDWR)
                    except:
                        pass
                    s.close()
                client_semaphore.release()
                break

# ------------------------ Ledger Sync ------------------------ #
def request_ledger_sync():
    local_count = get_ledger_count()
    if local_count == 0:
        last_hash = "0"
        last_prev_hash = "0"
    else:
        last_block = get_ledger_blocks(max(0, local_count - 1), 1)
        last_hash = last_block[0]["hash"] if last_block else "0"
        last_prev_hash = last_block[0]["prev_hash"] if last_block else "0"

    payload = {"last_hash": last_hash, "last_prev_hash": last_prev_hash, "total_count": local_count}
    logging.info(f"[Sync] Sending SYNC_REQUEST with last_hash={last_hash} total_count={local_count}")
    for sock in client_sockets[:]:
        try:
            sock.send(f"SYNC_REQUEST:{json.dumps(payload)}\n".encode())
        except:
            if sock in client_sockets:
                client_sockets.remove(sock)
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except:
                pass
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
            logging.info(f"[Sync] Peer up-to-date. Sent empty response.")
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
            end_index = min(len(result), start_index + BATCH_SIZE)

            for b in result[start_index:end_index]:
                if b["prev_hash"] != prev_hash:
                    break
                ts = b["timestamp"]
                if isinstance(ts, datetime):
                    ts = ts.strftime("%Y-%m-%d %H:%M:%S")
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
            logging.info(f"[Sync] No missing blocks. Chain is up-to-date.")
            safe_emit("sync_status", {"status": "synced"}, to_all=True)
            safe_emit("chat_history", get_ledger_blocks(0, 50), to_all=True)
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

        safe_emit("sync_status", {"status": "synced"}, to_all=True)
        safe_emit("chat_history", get_ledger_blocks(0, 50), to_all=True)

        if len(peer_blocks) == BATCH_SIZE:
            logging.info(f"[Sync] Full batch received. Requesting next batch...")
            request_ledger_sync()
    except Exception as e:
        logging.error(f"[Sync Response Error] {e}")
        safe_emit("sync_status", {"status": "error"}, to_all=True)

# ------------------------ Periodic Chain Validation ------------------------ #
def periodic_ledger_sync():
    while True:
        time.sleep(SYNC_INTERVAL)
        logging.info("[Sync] Checking chain validity...")
        if not validate_chain():
            logging.warning("[Sync] Local chain invalid. Requesting sync...")
            request_ledger_sync()

# peer_discovery.py

import socket
import psutil
import threading
import time

# === Configuration ===
TCP_SERVER_PORT = 5001
MAX_RETRIES = 5
RETRY_DELAY = 3
MAX_CLIENTS = 10
PEER_IPS = ["10.176.217.173"]
ADAPTER_NAME = 'Connect Tunnel'

# === Semaphores ===
client_semaphore = threading.Semaphore(MAX_CLIENTS)
peer_connect_semaphore = threading.Semaphore(5)

client_sockets = []  # Shared list to store connected sockets


# === Get IP Address from Network Adapter ===
def get_ip_of_adapter(adapter_name):
    adapter_found = False
    stats = psutil.net_if_stats()

    for interface_name, addrs in psutil.net_if_addrs().items():
        if isinstance(interface_name, str) and interface_name.lower() == adapter_name.lower():
            adapter_found = True
            is_up = stats.get(interface_name, None)
            if is_up and not is_up.isup:
                print(f"[!] Adapter '{adapter_name}' is down.")
                break

            for addr in addrs:
                if addr.family == socket.AF_INET:
                    return addr.address

            print(f"[!] Adapter '{adapter_name}' has no IPv4 address.")
            break

    if not adapter_found:
        print(f"[!] Adapter '{adapter_name}' not found.")

    return None


# === Handle Incoming Client Connection ===
def handle_client(conn, addr, socketio, messages):
    with client_semaphore:
        print(f"[+] Connection from {addr}")
        try:
            while True:
                data = conn.recv(1024).decode()
                if not data:
                    break

                print(data)
                messages.append(data)

                if ":" in data:
                    sender, msg = data.split(":", 1)
                    socketio.emit("receive_message", {"sender": sender.strip(), "message": msg.strip()})
                else:
                    socketio.emit("receive_message", {"sender": "Unknown", "message": data.strip()})
        except Exception as e:
            print(f"[!] Error receiving: {e}")
        finally:
            conn.close()
            print(f"[-] Disconnected: {addr}")


# === Start TCP Server ===
def start_tcp_server(local_ip, socketio, messages):
    def server_thread():
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((local_ip, TCP_SERVER_PORT))
        server.listen(5)
        print(f"[+] TCP Server listening on {local_ip}:{TCP_SERVER_PORT}")

        while True:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn, addr, socketio, messages), daemon=True).start()

    threading.Thread(target=server_thread, daemon=True).start()


# === Connect to Peers ===
def connect_to_peers():
    def connection_thread():
        for ip in PEER_IPS:
            for attempt in range(1, MAX_RETRIES + 1):
                with peer_connect_semaphore:
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect((ip, TCP_SERVER_PORT))
                        client_sockets.append(s)
                        print(f"[+] Connected to peer at {ip}:{TCP_SERVER_PORT}")
                        break
                    except Exception as e:
                        print(f"[!] Attempt {attempt}: Failed to connect to {ip}:{TCP_SERVER_PORT} - {e}")
                        time.sleep(RETRY_DELAY)

    threading.Thread(target=connection_thread, daemon=True).start()

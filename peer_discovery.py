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
PEER_IPS = ["10.176.153.235"]
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
def handle_client(conn, addr, socketio, messages, message_handler):
    with client_semaphore:
        print(f"[+] Connection from {addr}")
        try:
            while True:
                data = conn.recv(1024).decode()
                if not data:
                    break

                print(f"[+] Received from peer {addr}: {data}")
                messages.append(data)

                # Use the message handler function to process the message
                if message_handler:
                    message_handler(data, socketio)
                else:
                    # Fallback to old behavior
                    if ":" in data:
                        sender, msg = data.split(":", 1)
                        socketio.emit("receive_message", {
                            "sender": sender.strip(), 
                            "message": msg.strip(),
                            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
                        })
                    else:
                        socketio.emit("receive_message", {
                            "sender": "Unknown", 
                            "message": data.strip(),
                            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
                        })
                        
        except Exception as e:
            print(f"[!] Error receiving from {addr}: {e}")
        finally:
            conn.close()
            if conn in client_sockets:
                client_sockets.remove(conn)
            print(f"[-] Disconnected: {addr}")


# === Start TCP Server ===
def start_tcp_server(local_ip, socketio, messages, message_handler=None):
    def server_thread():
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            server.bind((local_ip, TCP_SERVER_PORT))
            server.listen(5)
            print(f"[+] TCP Server listening on {local_ip}:{TCP_SERVER_PORT}")

            while True:
                try:
                    conn, addr = server.accept()
                    client_sockets.append(conn)
                    threading.Thread(
                        target=handle_client, 
                        args=(conn, addr, socketio, messages, message_handler), 
                        daemon=True
                    ).start()
                except Exception as e:
                    print(f"[!] Error accepting connection: {e}")
                    
        except Exception as e:
            print(f"[!] Error starting TCP server: {e}")
        finally:
            server.close()

    threading.Thread(target=server_thread, daemon=True).start()


# === Connect to Peers ===
def connect_to_peers():
    def connection_thread():
        for ip in PEER_IPS:
            print(f"[i] Attempting to connect to peer at {ip}:{TCP_SERVER_PORT}")
            
            for attempt in range(1, MAX_RETRIES + 1):
                with peer_connect_semaphore:
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.settimeout(10)  # Set connection timeout
                        s.connect((ip, TCP_SERVER_PORT))
                        client_sockets.append(s)
                        print(f"[+] Connected to peer at {ip}:{TCP_SERVER_PORT}")
                        break
                    except Exception as e:
                        print(f"[!] Attempt {attempt}: Failed to connect to {ip}:{TCP_SERVER_PORT} - {e}")
                        if attempt < MAX_RETRIES:
                            time.sleep(RETRY_DELAY)
                        else:
                            print(f"[!] Failed to connect to {ip} after {MAX_RETRIES} attempts")

    threading.Thread(target=connection_thread, daemon=True).start()
import socket
import psutil
import threading
import time
from flask import Flask, render_template_string
from flask_socketio import SocketIO, emit
from collections import deque

# Configuration
CLIENT_NAME = 'Tanmay'
TCP_SERVER_PORT = 5001           # This client's socket server and peers' port
FLASK_WEB_PORT = 8001            # Web UI port
MAX_RETRIES = 5
RETRY_DELAY = 3

# 👇 Put peer IPs here directly
PEER_IPS = ["10.180.39.229"]

"""
/client_semaphore/
Purpose: Limits the number of simultaneous client connections your TCP server will handle at the same time.
Why?: Without limiting the number of concurrent connections, your server might get overwhelmed if too many clients
connect at once, leading to resource exhaustion (CPU, memory, sockets).
How it works?: When a new client connects, the server acquires the semaphore before handling that client.
If the maximum number of clients is reached (e.g., 10), new clients will wait until a slot is free.
"""
MAX_CLIENTS = 10  # Max concurrent incoming client connections allowed
client_semaphore = threading.Semaphore(MAX_CLIENTS) # Concurrent incoming clients

"""
/peer_connect_semaphore/
Purpose: Limits the number of simultaneous outgoing peer connection attempts.
Why?: When your program tries to connect to multiple peers, you don’t want to create too many connections at once,
which can cause spikes in resource usage or trigger network throttling/firewalls.
How it works?: Only 5 connection attempts happen at the same time. The rest wait their turn.
"""
peer_connect_semaphore = threading.Semaphore(5)  # Limit concurrent peer connections (optional) # Concurrent outgoing connects

def get_ip_of_adapter(adapter_name):
    """
    Returns the IPv4 address of the given network adapter name (case-insensitive).
    Verifies both presence and whether it's connected (interface is up and has an IPv4).
    """
    adapter_found = False
    stats = psutil.net_if_stats()

    for interface_name, addrs in psutil.net_if_addrs().items():
        print(f"[i] Checking interface: {interface_name}")
        if isinstance(interface_name, str) and interface_name.lower() == adapter_name.lower():
            adapter_found = True

            # Check if adapter is up
            is_up = stats.get(interface_name, None)
            if is_up and not is_up.isup:
                print(f"[!] Adapter '{adapter_name}' is present but not connected (status: down).")
                break

            # Check for an IPv4 address
            for addr in addrs:
                if addr.family == socket.AF_INET:
                    return addr.address
            print(f"[!] Adapter '{adapter_name}' is up but has no IPv4 address assigned.")
            break

    if not adapter_found:
        print(f"[!] Adapter '{adapter_name}' not found on this system.")

    return None

adapter_name = 'Connect Tunnel'
LOCAL_IP = get_ip_of_adapter(adapter_name)

if not LOCAL_IP:
    print(f"[!] Could not retrieve a usable IP from adapter '{adapter_name}'. Exiting.")
    exit(1)

print(f"[i] Using IP address from adapter '{adapter_name}': {LOCAL_IP}")
print(f"[i] Peers to connect: {PEER_IPS if PEER_IPS else 'None'}")

# Flask and SocketIO setup
app = Flask(__name__)
socketio = SocketIO(app)
messages = deque(maxlen=100)
client_sockets = []

# HTML Template with WebSocket
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
  <head>
    <title>{{ client_name }}</title>
    <style>
      body {
        font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
        margin: 0;
        padding: 0;
        background: #f0f2f5;
        display: flex;
        justify-content: center;
        align-items: center;
        height: 100vh;
      }

      .chat-container {
        width: 600px;
        height: 80vh;
        background: white;
        border-radius: 10px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
        display: flex;
        flex-direction: column;
        overflow: hidden;
      }

      .chat-header {
        background: #2e2e38;
        color: white;
        padding: 15px;
        font-size: 18px;
        font-weight: bold;
      }

      #chat {
        flex: 1;
        padding: 15px;
        overflow-y: auto;
        display: flex;
        flex-direction: column;
        gap: 10px;
      }

      .msg {
        max-width: 70%;
        padding: 10px 14px 20px;
        border-radius: 18px;
        font-size: 15px;
        line-height: 1.4;
        word-wrap: break-word;
        position: relative;
      }

      .you {
        align-self: flex-end;
        background: #ffe6008c;
        color: #2e2e38;
        border-top-right-radius: 18px;
        border-top-left-radius: 18px;
        border-bottom-left-radius: 18px;
        border-bottom-right-radius: 4px;
      }

      .peer {
        align-self: flex-start;
        background: #f0f0f0;
        color: #2e2e38;
        border-top-left-radius: 18px;
        border-top-right-radius: 18px;
        border-bottom-right-radius: 18px;
        border-bottom-left-radius: 4px;
      }

      .time {
        font-size: 11px;
        color: #555;
        opacity: 0.7;
        position: absolute;
        bottom: 4px;
        right: 10px;
      }

      .chat-footer {
        display: flex;
        border-top: 1px solid #ddd;
      }

      #msgInput {
        flex: 1;
        padding: 12px;
        border: none;
        font-size: 15px;
        outline: none;
      }

      button {
        background: #ffe600;
        border: none;
        color: #2e2e38;
        padding: 0 24px;
        cursor: pointer;
        font-size: 14px;
        font-weight: 700;
      }
    </style>
  </head>
  <body>
    <div class="chat-container">
      <div class="chat-header">{{ client_name }}</div>

      <div id="chat"></div>

      <form id="chatForm" class="chat-footer">
        <input
          type="text"
          id="msgInput"
          autocomplete="off"
          placeholder="Type a message..."
          required
        />
        <button type="submit">Send</button>
      </form>
    </div>

    <!-- Socket.IO client -->
    <script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
    <script>
      var socket = io();
      var chatDiv = document.getElementById("chat");
      var form = document.getElementById("chatForm");
      var input = document.getElementById("msgInput");

      form.onsubmit = function (e) {
        e.preventDefault();
        const msg = input.value.trim();
        if (msg) {
          socket.emit("send_message", msg);
          input.value = "";
        }
      };

      socket.on("receive_message", function (data) {
        const div = document.createElement("div");
        div.className =
          data.sender === "{{ client_name }}" ? "msg you" : "msg peer";

        const msgText = document.createElement("div");
        msgText.textContent =
          data.sender === "{{ client_name }}"
            ? "You: " + data.message
            : data.sender + ": " + data.message;

        const spanTime = document.createElement("span");
        spanTime.className = "time";
        spanTime.textContent = new Date().toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
        });

        div.appendChild(msgText);
        div.appendChild(spanTime);
        chatDiv.appendChild(div);
        chatDiv.scrollTop = chatDiv.scrollHeight;
      });
    </script>
  </body>
</html>

"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE, client_name=CLIENT_NAME)

@socketio.on('send_message')
def handle_send_message(msg):
    full_message = f"{CLIENT_NAME}: {msg}"
    messages.append(full_message)
    for sock in client_sockets:
        try:
            sock.send(full_message.encode())
        except Exception as e:
            print(f"[!] Error sending to peer: {e}")
    emit("receive_message", {"sender": CLIENT_NAME, "message": msg}, broadcast=True)

# TCP server to receive messages
def handle_client(conn, addr):
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

def start_tcp_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((LOCAL_IP, TCP_SERVER_PORT))
    server.listen(5)
    print(f"[+] TCP Server listening on {LOCAL_IP}:{TCP_SERVER_PORT}")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

# Connect to peers
def connect_to_peers():
    for ip in PEER_IPS:
        for attempt in range(1, MAX_RETRIES + 1):
            with peer_connect_semaphore:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((ip, TCP_SERVER_PORT))  # Use TCP_SERVER_PORT here
                    client_sockets.append(s)
                    print(f"[+] Connected to peer at {ip}:{TCP_SERVER_PORT}")
                    break
                except Exception as e:
                    print(f"[!] Attempt {attempt}: Failed to connect to {ip}:{TCP_SERVER_PORT} - {e}")
                    time.sleep(RETRY_DELAY)

# Start background threads
threading.Thread(target=start_tcp_server, daemon=True).start()
threading.Thread(target=connect_to_peers, daemon=True).start()

# Start Flask web app
if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=FLASK_WEB_PORT)
 
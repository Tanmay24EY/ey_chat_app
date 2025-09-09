from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from collections import deque
from peer_discovery import (
    get_ip_of_adapter,
    start_tcp_server,
    connect_to_peers,
    client_sockets,
)
from message_handler import (
    store_message,
    store_message_in_ledger,
    fetch_ledger,
    db,
)
import urllib

# === Configuration ===
CLIENT_NAME = 'Madhuri'
FLASK_WEB_PORT = 8001
ADAPTER_NAME = 'Connect Tunnel'

# === Get Local IP ===
LOCAL_IP = get_ip_of_adapter(ADAPTER_NAME)
if not LOCAL_IP:
    print(f"[!] Could not retrieve IP from adapter '{ADAPTER_NAME}'. Exiting.")
    exit(1)

print(f"[i] Using IP address from adapter '{ADAPTER_NAME}': {LOCAL_IP}")

# === Flask App Setup ===
app = Flask(__name__)
socketio = SocketIO(app)

# === Database Configuration ===
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=IN3594843W1\\SQLEXPRESS2019;"
    "DATABASE=chatdb;"
    "Trusted_Connection=yes;"
)
app.config['SQLALCHEMY_DATABASE_URI'] = f"mssql+pyodbc:///?odbc_connect={params}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize SQLAlchemy with Flask App
db.init_app(app)

# === Routes ===
@app.route('/')
def index():
    shared_ledger = fetch_ledger(100)
    return render_template('chat_page.html', client_name=CLIENT_NAME, messages=shared_ledger, FLASK_WEB_PORT=FLASK_WEB_PORT)

# === WebSocket Message Handler ===
@socketio.on('send_message')
def handle_send_message(msg):
    print(f'send messages: {msg}')
    # Store the message in the shared ledger and local message storage
    store_message(CLIENT_NAME, msg)
    store_message_in_ledger(CLIENT_NAME, msg)

    # Forward the message to all connected peers
    for sock in client_sockets:
        try:
            sock.send(f"{CLIENT_NAME}: {msg}".encode())
        except Exception as e:
            print(f"[!] Error sending to peer: {e}")

    # Send the message to all connected clients via WebSocket
    emit("receive_message", {
        "sender": CLIENT_NAME,
        "message": msg,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }, broadcast=True)

# === Peer Discovery Setup ===
messages = deque(maxlen=100)
start_tcp_server(LOCAL_IP, socketio, messages)
connect_to_peers()

# === Run Server ===
if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    socketio.run(app, host='0.0.0.0', port=FLASK_WEB_PORT)

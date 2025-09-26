import json
import logging
from datetime import datetime, timezone
from config import USER_TIMEZONE
import pytz

# Global reference to socketio instance
_socketio = None

def set_socketio(socketio_instance):
    """Set the global socketio instance"""
    global _socketio
    _socketio = socketio_instance

def safe_emit(event, data, to_all=False, room=None, **kwargs):
    """Safe emit function to handle SocketIO events"""
    global _socketio

    if _socketio is None:
        logging.error("[Emit Error] SocketIO instance not set")
        return

    try:
        payload = json.loads(json.dumps(data, default=str))
        if to_all:
            # Broadcast to all connected clients
            _socketio.emit(event, payload, room=room, **kwargs)
        else:
            # Emit to current request context (single client) or specific room
            _socketio.emit(event, payload, room=room, **kwargs)
    except Exception as e:
        logging.error(f"[Emit Error] Failed to emit {event}: {e}")


# --- Timezone Utility Functions ---
def get_utc_timestamp():
    """Get current UTC timestamp in ISO format for blockchain consistency"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def convert_utc_to_local(utc_timestamp_str, target_timezone=USER_TIMEZONE):
    """Convert UTC timestamp to user's local timezone for display"""
    try:
        # Parse UTC timestamp - handle both formats
        if isinstance(utc_timestamp_str, str):
            # Try ISO format first (2025-09-24T07:48:33)
            if 'T' in utc_timestamp_str:
                # Remove microseconds if present and handle Z suffix
                clean_timestamp = utc_timestamp_str.split('.')[0].replace('Z', '')
                utc_dt = datetime.strptime(clean_timestamp, "%Y-%m-%dT%H:%M:%S")
            else:
                # Standard format (2025-09-24 07:48:33)
                utc_dt = datetime.strptime(utc_timestamp_str, "%Y-%m-%d %H:%M:%S")
            utc_dt = utc_dt.replace(tzinfo=timezone.utc)
        else:
            # If it's already a datetime object, ensure it's UTC
            utc_dt = utc_timestamp_str.replace(tzinfo=timezone.utc) if utc_timestamp_str.tzinfo is None else utc_timestamp_str.astimezone(timezone.utc)

        # Convert to target timezone
        target_tz = pytz.timezone(target_timezone)
        local_dt = utc_dt.astimezone(target_tz)
        return local_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception as e:
        logging.error(f"[Timezone Error] {e}")
        return str(utc_timestamp_str)

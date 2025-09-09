# message_handler.py

import urllib
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from datetime import datetime

# === SQL Server connection via Windows Authentication ===
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=XE3253001W1\SQLEXPRESS;"
    "DATABASE=chatdb;"
    "Trusted_Connection=yes;"
)

# SQLAlchemy engine (optional use)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", echo=True)

# SQLAlchemy instance (used with Flask app)
db = SQLAlchemy()

# === Database Models ===

class Message(db.Model):
    __tablename__ = 'messages'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    sender = db.Column(db.String(255), nullable=False)
    timestamp = db.Column(db.String(20), nullable=False)
    message = db.Column(db.String, nullable=False)


class Ledger(db.Model):
    __tablename__ = 'ledger'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    sender = db.Column(db.String(255), nullable=False)
    timestamp = db.Column(db.String(20), nullable=False)
    message = db.Column(db.String, nullable=False)

# === Store a Message in the Database ===

def store_message(sender, message):
    """Store a new message in the messages table."""
    print(f'sender: {sender}, message: {message}')
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_message = Message(sender=sender, timestamp=timestamp, message=message)
    db.session.add(new_message)
    db.session.commit()

def store_message_in_ledger(sender, message):
    """Store a new message in the shared ledger."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_ledger_entry = Ledger(sender=sender, timestamp=timestamp, message=message)
    db.session.add(new_ledger_entry)
    db.session.commit()

def fetch_messages(limit=100):
    """Fetch the latest messages from the messages table."""
    return Message.query.order_by(Message.timestamp.asc()).limit(limit).all()

def fetch_ledger(limit=100):
    """Fetch the latest messages from the ledger table."""
    return Ledger.query.order_by(Ledger.timestamp.asc()).limit(limit).all()

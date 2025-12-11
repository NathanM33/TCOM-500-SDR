import socket
import sqlite3
import time
import os
from datetime import datetime

HOST = "127.0.0.1"
PORT = 30003

# -----------------------------
# Paths & Database Setup
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

timestamp = datetime.now().strftime("%m%d%y_%H%M%S")
DB_PATH = os.path.join(DATA_DIR, f"{timestamp}.db")

# Expected Mode S field order
FIELD_NAMES = [
    "mtype", "ttype", "sid", "aid", "hex", "flight", "date", "time",
    "callsign", "alt", "gspeed", "heading", "lat", "lon",
    "vrate", "squawk", "alert", "emergency", "spi", "grounded"
]

# Session timeout (minutes) — new session created if callsign reappears
SESSION_TIMEOUT_SEC = 20 * 60

# Tracks active sessions: {callsign: (session_id, last_seen_timestamp)}
active_sessions = {}


# -----------------------------
# Database Initialization
# -----------------------------
def initialize_database():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS aircraft_sessions (
            session_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            callsign     TEXT,
            icao_hex     TEXT,
            first_seen   TEXT,
            last_seen    TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS adsb_messages (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id   INTEGER,
            ts           TEXT,
            mtype        TEXT,
            ttype        TEXT,
            flight       TEXT,
            date         TEXT,
            time         TEXT,
            datel        TEXT,
            timel        TEXT,
            callsign     TEXT,
            alt          TEXT,
            gspeed       TEXT,
            heading      TEXT,
            lat          TEXT,
            lon          TEXT,
            squawk       TEXT,
            alert        TEXT,
            grounded     TEXT,
            FOREIGN KEY (session_id) REFERENCES aircraft_sessions(session_id)
        )
    """)

    conn.commit()
    conn.close()


# -----------------------------
# Parse incoming CSV message
# -----------------------------
def parse_message(line):
    parts = line.strip().split(",")

    parsed = {}
    for i, name in enumerate(FIELD_NAMES):
        parsed[name] = parts[i] if i < len(parts) else ""

    # Add datel and timel fields (copying date and time for now)
    parsed["datel"] = parsed.get("date", "")
    parsed["timel"] = parsed.get("time", "")

    return parsed


# -----------------------------
# Session Logic
# -----------------------------
def get_or_create_session(parsed, now_iso):
    callsign = parsed.get("callsign", "").strip()
    icao = parsed.get("hex", "").strip().upper()

    if callsign == "":
        return None  # No callsign → ignore for session grouping

    # Check for existing session
    if callsign in active_sessions:
        session_id, last_seen_ts = active_sessions[callsign]

        # Timeout check: callsign reappears after long gap = new session
        dt_last = datetime.fromisoformat(last_seen_ts)
        dt_now = datetime.fromisoformat(now_iso)

        if (dt_now - dt_last).total_seconds() > SESSION_TIMEOUT_SEC:
            return create_new_session(callsign, icao, now_iso)

        # Update existing session last_seen
        update_session_last_seen(session_id, now_iso)
        active_sessions[callsign] = (session_id, now_iso)
        return session_id

    # No existing session → create new one
    return create_new_session(callsign, icao, now_iso)


def create_new_session(callsign, icao_hex, now_iso):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO aircraft_sessions (callsign, icao_hex, first_seen, last_seen)
        VALUES (?, ?, ?, ?)
    """, (callsign, icao_hex, now_iso, now_iso))

    session_id = cur.lastrowid
    conn.commit()
    conn.close()

    active_sessions[callsign] = (session_id, now_iso)
    return session_id


def update_session_last_seen(session_id, now_iso):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        UPDATE aircraft_sessions
        SET last_seen = ?
        WHERE session_id = ?
    """, (now_iso, session_id))
    conn.commit()
    conn.close()


# -----------------------------
# Insert ADS-B message
# -----------------------------
def insert_message(parsed):
    # Filtering rules
    if parsed.get("flight", "") == "111111" and parsed.get("callsign", "").strip() == "":
        return

    # Build full timestamp: YYYY-MM-DD HH:MM:SS
    try:
        dt = datetime.strptime(
            parsed["date"] + " " + parsed["time"],
            "%Y/%m/%d %H:%M:%S.%f"
        )
    except:
        dt = datetime.now()

    ts_iso = dt.strftime("%Y-%m-%d %H:%M:%S")

    # Determine which session this message belongs to
    session_id = get_or_create_session(parsed, ts_iso)
    if session_id is None:
        return

    # Insert message
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO adsb_messages (
            session_id, ts, mtype, ttype, flight, date, time, datel, timel,
            callsign, alt, gspeed, heading, lat, lon, squawk, alert, grounded
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        session_id,
        ts_iso,
        parsed.get("mtype", ""),
        parsed.get("ttype", ""),
        parsed.get("flight", ""),
        parsed.get("date", ""),
        parsed.get("time", ""),
        parsed.get("datel", ""),
        parsed.get("timel", ""),
        parsed.get("callsign", ""),
        parsed.get("alt", ""),
        parsed.get("gspeed", ""),
        parsed.get("heading", ""),
        parsed.get("lat", ""),
        parsed.get("lon", ""),
        parsed.get("squawk", ""),
        parsed.get("alert", ""),
        parsed.get("grounded", ""),
    ))

    conn.commit()
    conn.close()


# -----------------------------
# Network Reader w/ Auto-Reconnect
# -----------------------------
def connect_and_listen():
    while True:
        try:
            print(f"Connecting to {HOST}:{PORT} ...")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((HOST, PORT))
                print("Connected. Listening...\n")

                buffer = b""

                while True:
                    data = s.recv(4096)
                    if not data:
                        print("Server disconnected. Reconnecting...")
                        break

                    buffer += data
                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        try:
                            text = line.decode(errors="replace")
                            parsed = parse_message(text)
                            insert_message(parsed)
                        except Exception as e:
                            print(f"Error parsing message: {e}")

        except Exception as e:
            print(f"Connection failed ({e}). Retrying in 3 seconds...")
            time.sleep(3)


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    initialize_database()
    connect_and_listen()

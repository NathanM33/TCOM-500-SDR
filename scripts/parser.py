import socket
import time
import os
import sqlite3
from datetime import datetime

HOST = "127.0.0.1"
PORT = 30003
RECONNECT_DELAY = 3

# Determine database path, relative to script location
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

# Database filename: MMDDYY_HHMMSS.db
DB_PATH = os.path.join(DATA_DIR, "flightsdata.db")

# If DB already exists, delete it for a fresh start
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print(f"Deleted existing database at {DB_PATH}")

print(f"Using database: {DB_PATH}")

##############################################
# Database Setup
##############################################

def initialize_database():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Flights table: one row per HEX code
    cur.execute("""
        CREATE TABLE IF NOT EXISTS flights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hex TEXT UNIQUE,
            callsign TEXT,
            alt TEXT,
            gspeed TEXT,
            heading TEXT,
            lat TEXT,
            lon TEXT,
            grounded TEXT,
            created_at TEXT
        )
    """)

    # Messages table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS smessages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            flight_id INTEGER,
            hex TEXT,
            date TEXT,
            time TEXT,
            callsign TEXT,
            alt TEXT,
            gspeed TEXT,
            heading TEXT,
            lat TEXT,
            lon TEXT,
            grounded TEXT,
            created_at TEXT,
            FOREIGN KEY(flight_id) REFERENCES flights(id)
        )
    """)
    
    # Flight positions table (history)
    cur.execute("""
        CREATE TABLE flight_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hex TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            lat REAL,
            lon REAL,
            alt INTEGER,
            heading REAL,
            gspeed REAL
        )
    """)

    conn.commit()
    conn.close()


def get_or_create_flight(conn, hexcode):
    """
    Returns flight_id for this ICAO hex code.
    If not present, create a new flight entry.
    """
    cur = conn.cursor()

    cur.execute("SELECT id FROM flights WHERE hex = ?", (hexcode,))
    row = cur.fetchone()

    if row:
        return row[0]

    # Create new flight entry
    created_at = datetime.utcnow().isoformat()
    cur.execute(
        "INSERT INTO flights (hex, created_at) VALUES (?, ?)",
        (hexcode, created_at)
    )
    conn.commit()
    return cur.lastrowid


def update_flight_fields(conn, flight_id, fields):
    """
    Update flights table with non-blank fields and
    append a position record to flight_positions when available.
    """
    cur = conn.cursor()

    # ---- 1. Update current flight state (existing logic) ----
    update_map = {
        "callsign": fields.get("callsign"),
        "alt": fields.get("alt"),
        "gspeed": fields.get("gspeed"),
        "heading": fields.get("heading"),
        "lat": fields.get("lat"),
        "lon": fields.get("lon"),
        "grounded": fields.get("grounded"),
    }

    updates = []
    values = []

    for key, val in update_map.items():
        if val not in ("", None):
            updates.append(f"{key} = ?")
            values.append(val)

    if updates:
        sql = f"UPDATE flights SET {', '.join(updates)} WHERE id = ?"
        values.append(flight_id)
        cur.execute(sql, values)

    # ---- 2. Insert into flight_positions (NEW) ----
    lat = fields.get("lat")
    lon = fields.get("lon")

    # Only store history if we have a valid position
    if lat not in ("", None) and lon not in ("", None):
        timestamp = f"{fields.get('date')} {fields.get('time')}"

        cur.execute("""
            INSERT INTO flight_positions (
                hex,
                timestamp,
                lat,
                lon,
                alt,
                heading,
                gspeed
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            fields.get("hex"),
            timestamp,
            lat,
            lon,
            fields.get("alt"),
            fields.get("heading"),
            fields.get("gspeed")
        ))

    conn.commit()



##############################################
# Message Parsing
##############################################

def parse_message(raw):
    """
    Parses SBS-1/BaseStation ADS-B messages.
    """
    parts = raw.strip().split(',')

    while len(parts) < 22:
        parts.append("")

    return {
        "mtype": parts[0],
        "ttype": parts[1],
        "hex": parts[4].strip().upper(),
        "flight": parts[5],
        "date": parts[6],
        "time": parts[7],
        "callsign": parts[10],
        "alt": parts[11],
        "gspeed": parts[12],
        "heading": parts[13],
        "lat": parts[14],
        "lon": parts[15],
        "grounded": parts[21]
    }


##############################################
# Main Loop
##############################################

def main():
    print("Initializing database:", DB_PATH)
    initialize_database()

    while True:
        try:
            print(f"Connecting to {HOST}:{PORT}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((HOST, PORT))
            print("Connected.")

            with s, sqlite3.connect(DB_PATH) as conn:
                while True:
                    data = s.recv(4096)
                    if not data:
                        raise ConnectionError("Disconnected")

                    for raw in data.decode(errors="ignore").splitlines():
                        if not raw.strip():
                            continue

                        fields = parse_message(raw)

                        # Only store MSG messages
                        if fields["mtype"] != "MSG":
                            continue

                        # Determine grouping key (ICAO HEX code)
                        hexcode = fields["hex"] if fields["hex"] else "UNKNOWN_HEX"

                        # Get or create flight record
                        flight_id = get_or_create_flight(conn, hexcode)

                        # NEW FUNCTIONALITY: update flight fields if non-empty
                        update_flight_fields(conn, flight_id, fields)

                        # Insert message into smessages table
                        cur = conn.cursor()
                        cur.execute("""
                            INSERT INTO smessages (
                                flight_id, hex, date, time, callsign, alt,
                                gspeed, heading, lat, lon, grounded, created_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            flight_id,
                            fields["hex"],
                            fields["date"],
                            fields["time"],
                            fields["callsign"],
                            fields["alt"],
                            fields["gspeed"],
                            fields["heading"],
                            fields["lat"],
                            fields["lon"],
                            fields["grounded"],
                            datetime.utcnow().isoformat()
                        ))

                        conn.commit()

        except Exception as e:
            print("Connection lost / error:", e)
            print(f"Reconnecting in {RECONNECT_DELAY} seconds...")
            time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    main()

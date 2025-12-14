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
    Update flight entry with non-blank fields:
      callsign, alt, gspeed, heading, lat, lon, grounded
    """
    cur = conn.cursor()

    # Fields that may update the flights row
    update_map = {
        "callsign": fields["callsign"],
        "alt": fields["alt"],
        "gspeed": fields["gspeed"],
        "heading": fields["heading"],
        "lat": fields["lat"],
        "lon": fields["lon"],
        "grounded": fields["grounded"]
    }

    # Build dynamic SQL update of only non-empty fields
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

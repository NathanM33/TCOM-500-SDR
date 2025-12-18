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
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "flightsdata.db")


# -----------------------------
# Database Initialization
# -----------------------------
def initialize_database():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS flights (
            hex TEXT PRIMARY KEY,
            callsign TEXT,
            alt TEXT,
            gspeed TEXT,
            heading TEXT,
            lat TEXT,
            lon TEXT,
            grounded TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS flight_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hex TEXT,
            timestamp INTEGER,
            lat REAL,
            lon REAL,
            alt TEXT,
            heading TEXT,
            gspeed TEXT
        )
    """)

    conn.commit()
    conn.close()


# -----------------------------
# SBS-1 Message Parsing
# -----------------------------
def parse_message(raw):
    parts = raw.strip().split(',')

    # Pad to expected length
    while len(parts) < 22:
        parts.append("")

    return {
        "mtype": parts[0],
        "hex": parts[4].strip().upper(),
        "callsign": parts[10].strip(),
        "alt": parts[11],
        "gspeed": parts[12],
        "heading": parts[13],
        "lat": parts[14],
        "lon": parts[15],
        "grounded": parts[21],
        "timestamp": int(time.time() * 1000)
    }


# -----------------------------
# Main Loop
# -----------------------------
def main():
    print("Initializing database:", DB_PATH)
    initialize_database()

    while True:
        try:
            print(f"Connecting to dump1090 at {HOST}:{PORT} ...")

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((HOST, PORT))
                print("Connected. Listening...\n")

                conn = sqlite3.connect(DB_PATH)
                cur = conn.cursor()

                buffer = b""

                while True:
                    data = s.recv(4096)
                    if not data:
                        print("dump1090 disconnected.")
                        break

                    buffer += data

                    try:
                        lines = buffer.decode(errors="ignore").splitlines()
                        buffer = b""
                    except Exception:
                        continue

                    for raw in lines:
                        if not raw.strip():
                            continue

                        fields = parse_message(raw)

                        # Only MSG packets
                        if fields["mtype"] != "MSG":
                            continue

                        hexcode = fields["hex"]
                        if not hexcode:
                            continue

                        ts = fields["timestamp"]

                        # -----------------------------
                        # Update flights table
                        # -----------------------------
                        cur.execute("""
                            INSERT OR IGNORE INTO flights (hex)
                            VALUES (?)
                        """, (hexcode,))

                        updates = []
                        values = []

                        for key in (
                            "timestamp", "callsign", "alt", "gspeed",
                            "heading", "lat", "lon", "grounded"
                        ):
                            val = fields.get(key)
                            if val not in ("", None):
                                updates.append(f"{key} = ?")
                                values.append(val)

                        if updates:
                            sql = f"""
                                UPDATE flights
                                SET {', '.join(updates)}
                                WHERE hex = ?
                            """
                            cur.execute(sql, values + [hexcode])

                        # -----------------------------
                        # Insert track history
                        # -----------------------------
                        if fields["lat"] and fields["lon"]:
                            cur.execute("""
                                INSERT INTO flight_positions (
                                    hex, timestamp, lat, lon, alt, heading, gspeed
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, (
                                hexcode,
                                ts,
                                fields["lat"],
                                fields["lon"],
                                fields["alt"],
                                fields["heading"],
                                fields["gspeed"]
                            ))

                        conn.commit()

        except Exception as e:
            print(f"Parser error: {e}")
            print("Retrying in 3 seconds...")
            time.sleep(3)


# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    main()

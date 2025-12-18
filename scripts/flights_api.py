from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import sqlite3

# Path to your SQLite database
DB_PATH = "./data/flightsdata.db"

# Create FastAPI app
app = FastAPI()

# Allow browser access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/flights")
def flights():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT hex, callsign, alt, gspeed, heading, lat, lon, grounded
        FROM flights
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """)
    rows = cur.fetchall()
    conn.close()

    return [
        {
            "hex": r[0],
            "callsign": r[1],
            "alt": r[2],
            "gspeed": r[3],
            "heading": r[4],
            "lat": float(r[5]) if r[5] else None,
            "lon": float(r[6]) if r[6] else None,
            "grounded": r[7]
        } for r in rows
    ]
    
@app.get("/api/flights/{hex}")
def get_flight_by_hex(hex: str):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT
            hex,
            callsign,
            lat,
            lon,
            alt,
            heading,
            gspeed,
            grounded
        FROM flights
        WHERE UPPER(hex) = UPPER(?)
        LIMIT 1
    """, (hex,))

    row = cur.fetchone()
    conn.close()

    if not row:
        return {"found": False}

    return {"found": True, "flight": dict(row)}


@app.get("/api/track/{hex}")
def get_track(hex: str, limit: int = 300):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT lat, lon, heading, timestamp
        FROM flight_positions
        WHERE hex = ?
        ORDER BY timestamp DESC
        LIMIT ?
    """, (hex, limit))

    rows = cur.fetchall()
    conn.close()

    return list(reversed([dict(r) for r in rows]))


# --- Run server when executed directly ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("flights_api:app", host="127.0.0.1", port=8000, reload=False)
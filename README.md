# TCOM-500-SDR Project Objectives Statement

The objective of this project is to build a Dockerized app which:
- Uses an ADS-B decoder and SDR to receive and parse Mode S (air traffic conrtrol) messages,
- Extracts pertinent fields, and persists these messages to a relational database,
- Displays an air traffic control map capable of live capture and aircraft track replay,
- Implements a simple callsign/ICAO search mechanism that provides track replay,

The final product has clear industrial and enthusiast applications in the aviation industry.

To achieve these objectives, the portable app design will incorporate:
- A Mode S message decoder component (dump1090), which connects to an SDR + 1090MHz antenna dongle
- A python script message parser component which receives decoded messages from dump1090's network feature and writes to a lightweight backend relational SQL database, grouping and ordering messages from the same flight
- An SQLite database
- A web user-interface which pulls and displays live and replay data from the relational database.

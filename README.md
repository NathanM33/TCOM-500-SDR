# TCOM-500-SDR Project Objectives Statement

The objective of this project is to build a Dockerized app which:
- Uses an ADS-B decoder and SDR to receive and parse Mode S (air traffic conrtrol) messages,
- Extracts pertinent fields, and persists these messages to a relational database,
- Displays an air traffic control map capable of live capture and aircraft track replay,
- Implements a simple callsign/ICAO search mechanism that provides track replay,

The final product has clear industrial and enthusiast applications in the aviation industry.

To achieve these objectives, the portable app design will incorporate a Mode S message decoder component, a message parser component, a backend relational database that stores the parsed messages, relating the messages based on a track schema, and a web user-interface which pulls and displays live and replay data from the relational database.

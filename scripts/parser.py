import socket

HOST = "127.0.0.1"
PORT = 30003

def start_client():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        print(f"Connecting to {HOST}:{PORT}...")
        s.connect((HOST, PORT))
        print("Connected. Listening for messages...\n")

        while True:
            data = s.recv(4096)
            if not data:
                print("Server closed the connection.")
                break

            message = data.decode(errors="replace")
            print(message.rstrip())

if __name__ == "__main__":
    start_client()

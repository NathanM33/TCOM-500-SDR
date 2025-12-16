import asyncio
import sys
import subprocess
from pathlib import Path

# Check for / install FastAPI & uvicorn
def ensure_packages():
    try:
        import fastapi
        import uvicorn
        print("FastAPI and uvicorn already installed.")
    except ImportError:
        print("FastAPI or uvicorn not found. Installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "fastapi", "uvicorn"])
        print("Installation complete.")

ensure_packages()

# Resolve absolute paths for scripts
BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR / "scripts"
PARSER_SCRIPT = SCRIPTS_DIR / "parser.py"
API_SCRIPT = SCRIPTS_DIR / "flights_api.py"
DUMP1090_SCRIPT = SCRIPTS_DIR / "dump1090" / "dump1090"

PROCESSES = [
    ("Parser", [sys.executable, str(PARSER_SCRIPT)]),
    ("Flights API", [sys.executable, str(API_SCRIPT)]),
    ("Dump1090", [str(DUMP1090_SCRIPT), "--net"])
]

async def run_process(name, cmd):
    while True:
        print(f"[{name}] Starting: {' '.join(cmd)}")
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT
            )
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                print(f"[{name}] {line.decode().rstrip()}")
            await process.wait()
            print(f"[{name}] exited with code {process.returncode}")
        except Exception as e:
            print(f"[{name}] crashed with exception: {e}")

        print(f"[{name}] Retrying in 3 seconds...")
        await asyncio.sleep(3)

async def main():
    await asyncio.gather(*(run_process(name, cmd) for name, cmd in PROCESSES))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down all processes...")

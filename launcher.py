import asyncio
import sys
import subprocess
from pathlib import Path

# --- Auto-install FastAPI & uvicorn ---
def ensure_packages():
    try:
        import fastapi  # noqa: F401
        import uvicorn  # noqa: F401
        print("FastAPI and uvicorn already installed.")
    except ImportError:
        print("FastAPI or uvicorn not found. Installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "fastapi", "uvicorn"])
        print("Installation complete.")


ensure_packages()


# --- Paths ---
SCRIPTS_DIR = Path("./scripts")
DUMP1090_DIR = Path("./scripts/dump1090")

ADSB_SCRIPT = SCRIPTS_DIR / "parser.py"
API_SCRIPT = SCRIPTS_DIR / "flights_api.py"
DUMP1090_CMD = DUMP1090_DIR / "dump1090"

PROCESSES = [
    ("ADSB Listener", [sys.executable, str(ADSB_SCRIPT)]),
    ("Flight API", [sys.executable, str(API_SCRIPT)]),
    ("Dump1090", [str(DUMP1090_CMD), "--net"])  # headless network mode
]


# --- Async process runner with auto-restart ---
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

        print(f"[{name}] Restarting in 3 seconds...")
        await asyncio.sleep(3)


# --- Main asyncio loop ---
async def main():
    await asyncio.gather(*(run_process(name, cmd) for name, cmd in PROCESSES))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down all processes...")

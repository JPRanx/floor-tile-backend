#!/usr/bin/env python3
"""
Server management script.

Ensures only one instance runs at a time by killing zombies before starting.

Usage:
    python server.py start   # Start server (kills existing first, Ctrl+C to stop)
    python server.py stop    # Stop server
    python server.py status  # Check if running
"""

import os
import sys
import signal
import subprocess
import time
from pathlib import Path

# Configuration
PID_FILE = Path(__file__).parent / "server.pid"
PORT = 8000
HOST = "0.0.0.0"


def find_python_processes():
    """Find all Python processes running uvicorn."""
    try:
        if os.name == 'nt':  # Windows
            result = subprocess.run(
                ["tasklist", "/FI", "IMAGENAME eq python.exe", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True
            )
            lines = result.stdout.strip().split('\n')
            processes = []
            for line in lines:
                if line and 'python.exe' in line.lower():
                    parts = line.split(',')
                    if len(parts) >= 2:
                        pid = parts[1].strip('"')
                        try:
                            processes.append(int(pid))
                        except ValueError:
                            pass
            return processes
        else:  # Unix/Linux/Mac
            result = subprocess.run(
                ["pgrep", "-f", "uvicorn.*main:app"],
                capture_output=True,
                text=True
            )
            return [int(pid) for pid in result.stdout.strip().split('\n') if pid]
    except Exception:
        return []


def kill_process(pid):
    """Kill a process by PID."""
    try:
        if os.name == 'nt':  # Windows
            subprocess.run(
                ["taskkill", "/F", "/PID", str(pid)],
                capture_output=True,
                check=False
            )
        else:  # Unix/Linux/Mac
            os.kill(pid, signal.SIGTERM)
        return True
    except Exception:
        return False


def kill_existing():
    """Kill any existing server processes."""
    killed = False

    # Kill by PID file
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            if kill_process(pid):
                print(f"[OK] Killed existing server (PID: {pid})")
                killed = True
        except (ProcessLookupError, ValueError, OSError):
            pass
        PID_FILE.unlink()

    # Kill by process search
    pids = find_python_processes()
    for pid in pids:
        if kill_process(pid):
            print(f"[OK] Killed orphaned server (PID: {pid})")
            killed = True

    if killed:
        time.sleep(1)

    return killed


def check_port():
    """Check if port is available."""
    try:
        if os.name == 'nt':  # Windows
            result = subprocess.run(
                ["netstat", "-ano"],
                capture_output=True,
                text=True
            )
            for line in result.stdout.split('\n'):
                if f":{PORT}" in line and "LISTENING" in line:
                    return False
        else:  # Unix/Linux/Mac
            result = subprocess.run(
                ["lsof", "-i", f":{PORT}"],
                capture_output=True,
                text=True
            )
            if result.stdout.strip():
                return False
        return True
    except Exception:
        return True


def get_status():
    """Check if server is running."""
    # Check PID file
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            # Check if process exists
            if os.name == 'nt':  # Windows
                result = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                    capture_output=True,
                    text=True
                )
                if str(pid) in result.stdout:
                    return True, pid
            else:  # Unix/Linux/Mac
                try:
                    os.kill(pid, 0)  # Check if process exists
                    return True, pid
                except OSError:
                    pass
        except (ValueError, OSError):
            pass

    # Check port
    if not check_port():
        return True, None  # Something is on the port

    return False, None


def start_server():
    """Start the server."""
    print("Starting server...")

    # Kill existing
    if kill_existing():
        print()

    # Verify port is available
    if not check_port():
        print(f"[ERROR] Port {PORT} is still in use!")
        print("   Wait a moment or run: python server.py stop")
        sys.exit(1)

    try:
        # Start uvicorn
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m", "uvicorn",
                "main:app",
                "--host", HOST,
                "--port", str(PORT),
                "--reload"
            ],
            cwd=Path(__file__).parent
        )

        # Save PID
        PID_FILE.write_text(str(proc.pid))
        print(f"[OK] Server started (PID: {proc.pid})")
        print(f"[OK] API: http://localhost:{PORT}")
        print(f"[OK] Docs: http://localhost:{PORT}/docs")
        print("\nPress Ctrl+C to stop")

        try:
            proc.wait()
        except KeyboardInterrupt:
            print("\n\nShutting down...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            if PID_FILE.exists():
                PID_FILE.unlink()
            print("[OK] Server stopped")

    except FileNotFoundError:
        print("[ERROR] uvicorn not found!")
        print("   Install: pip install uvicorn")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        if PID_FILE.exists():
            PID_FILE.unlink()
        sys.exit(1)


def stop_server():
    """Stop the server."""
    print("Stopping server...")
    killed = kill_existing()
    if not killed:
        print("[OK] No server processes found")
    else:
        print("[OK] Server stopped")


def show_status():
    """Show server status."""
    running, pid = get_status()
    if running:
        if pid:
            print(f"[OK] Server is running (PID: {pid})")
            print(f"     http://localhost:{PORT}")
        else:
            print(f"[OK] Server is running on port {PORT}")
    else:
        print("[NOT RUNNING] Server is not running")
        print("              Start with: python server.py start")


def main():
    """Main entry point."""
    command = sys.argv[1] if len(sys.argv) > 1 else "start"

    if command == "start":
        start_server()
    elif command == "stop":
        stop_server()
    elif command == "status":
        show_status()
    else:
        print("Usage: python server.py [start|stop|status]")
        sys.exit(1)


if __name__ == "__main__":
    main()
"""
List all ports for testing.
"""

import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from config import get_supabase_client


def list_ports():
    """List all ports."""
    db = get_supabase_client()

    result = db.table("ports").select("*").order("type", desc=False).execute()

    print("Available ports:\n")

    origin_ports = [p for p in result.data if p["type"] == "ORIGIN"]
    destination_ports = [p for p in result.data if p["type"] == "DESTINATION"]

    print("ORIGIN PORTS:")
    for port in origin_ports:
        print(f"  - {port['name']}, {port['country']}: {port['id']}")

    print("\nDESTINATION PORTS:")
    for port in destination_ports:
        print(f"  - {port['name']}, {port['country']}: {port['id']}")

    if origin_ports and destination_ports:
        print(f"\nFor testing, use:")
        print(f"  origin_port_id: {origin_ports[0]['id']}")
        print(f"  destination_port_id: {destination_ports[0]['id']}")


if __name__ == "__main__":
    list_ports()
"""
Seed ports table with initial data.

Run this script to populate the ports table with origin and destination ports.
"""

import sys
from pathlib import Path

# Add backend to path so we can import modules
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from config import get_supabase_client
import structlog

logger = structlog.get_logger(__name__)


def seed_ports():
    """Insert initial port data."""
    db = get_supabase_client()

    # Check if ports already exist
    existing = db.table("ports").select("id", count="exact").execute()

    if existing.count > 0:
        logger.info("ports_already_seeded", count=existing.count)
        print(f"✓ Ports table already has {existing.count} ports")
        return

    # Origin ports (Colombia)
    origin_ports = [
        {"name": "Cartagena", "country": "Colombia", "type": "ORIGIN", "unlocode": "COCTG"},
        {"name": "Barranquilla", "country": "Colombia", "type": "ORIGIN", "unlocode": "COBAQ"},
    ]

    # Destination ports (Central America)
    destination_ports = [
        {"name": "Santo Tomas de Castilla", "country": "Guatemala", "type": "DESTINATION", "unlocode": "GTSTC"},
        {"name": "Puerto Quetzal", "country": "Guatemala", "type": "DESTINATION", "unlocode": "GTPRQ"},
        {"name": "Puerto Barrios", "country": "Guatemala", "type": "DESTINATION", "unlocode": "GTPBR"},
        {"name": "Puerto Cortes", "country": "Honduras", "type": "DESTINATION", "unlocode": "HNPCR"},
        {"name": "Acajutla", "country": "El Salvador", "type": "DESTINATION", "unlocode": "SVAQJ"},
    ]

    all_ports = origin_ports + destination_ports

    try:
        result = db.table("ports").insert(all_ports).execute()

        logger.info("ports_seeded", count=len(result.data))
        print(f"✓ Successfully seeded {len(result.data)} ports")

        # Print first origin and destination for reference
        print("\nSample ports:")
        print(f"  Origin: {result.data[0]['name']} ({result.data[0]['id']})")
        print(f"  Destination: {result.data[2]['name']} ({result.data[2]['id']})")

        return result.data

    except Exception as e:
        logger.error("seed_ports_failed", error=str(e))
        print(f"✗ Failed to seed ports: {e}")
        raise


if __name__ == "__main__":
    print("Seeding ports table...")
    seed_ports()
    print("\nDone!")
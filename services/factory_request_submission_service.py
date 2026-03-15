"""
Factory Request Submission Service.

Records factory production request exports (Excel) to a ledger table.
Follows the same pattern as warehouse_order_service.
"""

import uuid
from datetime import datetime, timezone

import structlog

from config.database import get_supabase_client

logger = structlog.get_logger(__name__)


class FactoryRequestSubmissionService:
    """Tracks factory request submissions."""

    def __init__(self):
        self.client = get_supabase_client()

    def record_submission(self, data: dict) -> dict:
        """Record a factory request submission and its items."""
        submission_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        # Insert submission record
        submission = {
            "id": submission_id,
            "factory_id": data["factory_id"],
            "factory_name": data["factory_name"],
            "total_pallets": data["total_pallets"],
            "total_m2": float(data["total_m2"]),
            "total_containers": data["total_containers"],
            "product_count": len(data["items"]),
            "notes": data.get("notes"),
            "submitted_at": now.isoformat(),
        }

        result = (
            self.client.table("factory_request_submissions")
            .insert(submission)
            .execute()
        )

        if not result.data:
            raise ValueError("Failed to insert factory request submission")

        # Insert items
        items = [
            {
                "id": str(uuid.uuid4()),
                "submission_id": submission_id,
                "product_id": item["product_id"],
                "sku": item["sku"],
                "pallets": item["pallets"],
                "m2": float(item["m2"]),
                "urgency": item["urgency"],
            }
            for item in data["items"]
        ]

        if items:
            self.client.table("factory_request_submission_items").insert(items).execute()

        logger.info(
            "factory_request_submission_recorded",
            submission_id=submission_id,
            factory_id=data["factory_id"],
            products=len(items),
            total_pallets=data["total_pallets"],
        )

        return {
            "id": submission_id,
            "factory_id": data["factory_id"],
            "factory_name": data["factory_name"],
            "total_pallets": data["total_pallets"],
            "total_m2": data["total_m2"],
            "total_containers": data["total_containers"],
            "product_count": len(items),
            "submitted_at": now,
            "notes": data.get("notes"),
        }

    def get_last_submission(self, factory_id: str) -> dict | None:
        """Get the most recent submission for a factory."""
        result = (
            self.client.table("factory_request_submissions")
            .select("id, submitted_at, total_pallets, total_m2, total_containers, product_count")
            .eq("factory_id", factory_id)
            .order("submitted_at", desc=True)
            .limit(1)
            .execute()
        )

        if not result.data:
            return None

        row = result.data[0]
        submitted_at = datetime.fromisoformat(row["submitted_at"].replace("Z", "+00:00"))
        days_ago = (datetime.now(timezone.utc) - submitted_at).days

        return {
            "id": row["id"],
            "submitted_at": submitted_at,
            "total_pallets": row["total_pallets"],
            "total_m2": row["total_m2"],
            "total_containers": row["total_containers"],
            "product_count": row["product_count"],
            "days_ago": days_ago,
        }


_service: FactoryRequestSubmissionService | None = None


def get_factory_request_submission_service() -> FactoryRequestSubmissionService:
    global _service
    if not _service:
        _service = FactoryRequestSubmissionService()
    return _service

"""
Draft service for boat-factory draft management.

Handles CRUD operations for boat_factory_drafts and their draft_items.
Drafts represent product selections being prepared for a factory order on a specific boat.
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Optional

import structlog

from config import get_supabase_client
from exceptions import DatabaseError

logger = structlog.get_logger(__name__)


class DraftService:
    """
    Draft business logic.

    Handles CRUD for boat_factory_drafts and nested draft_items.
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.drafts_table = "boat_factory_drafts"
        self.items_table = "draft_items"

    def _fetch_items(self, draft_id: str) -> list[dict]:
        """
        Fetch all draft_items for a given draft_id.

        Args:
            draft_id: Draft UUID

        Returns:
            List of item dicts
        """
        result = (
            self.db.table(self.items_table)
            .select("*")
            .eq("draft_id", draft_id)
            .execute()
        )
        return result.data

    @staticmethod
    def _compute_items_hash(items: list[dict]) -> str:
        """
        Compute a deterministic hash of draft items for change detection.

        Only selection-relevant fields (product_id, selected_pallets) are included
        so that re-ordering items or changing notes alone does not trigger cascade.
        Items are sorted by product_id for determinism.

        Args:
            items: List of item dicts (from DB rows or incoming payload)

        Returns:
            MD5 hex digest string
        """
        normalized = sorted(
            [
                {
                    "product_id": item.get("product_id", ""),
                    "selected_pallets": item.get("selected_pallets", 0),
                }
                for item in items
            ],
            key=lambda x: x["product_id"],
        )
        content = json.dumps(normalized, sort_keys=True)
        return hashlib.md5(content.encode()).hexdigest()

    def _attach_items(self, draft: dict) -> dict:
        """
        Attach items list to a draft dict.

        Args:
            draft: Draft dict

        Returns:
            Draft dict with "items" key added
        """
        draft["items"] = self._fetch_items(draft["id"])
        return draft

    def _flag_later_drafts(self, boat_id: str, factory_id: str, reason: str) -> int:
        """
        Flag all drafts for later boats (same factory) as action_needed.

        Args:
            boat_id: The boat whose draft was modified
            factory_id: Factory UUID
            reason: Text explaining why flagging (e.g., "Borrador anterior modificado")

        Returns:
            Number of drafts flagged
        """
        try:
            # Get this boat's departure date
            boat_result = (
                self.db.table("boat_schedules")
                .select("departure_date")
                .eq("id", boat_id)
                .execute()
            )
            if not boat_result.data:
                return 0
            this_departure = boat_result.data[0]["departure_date"]

            # Find all drafts for this factory
            drafts_result = (
                self.db.table(self.drafts_table)
                .select("id, boat_id, status")
                .eq("factory_id", factory_id)
                .neq("boat_id", boat_id)
                .execute()
            )
            if not drafts_result.data:
                return 0

            # Get departure dates for those boats
            other_boat_ids = [d["boat_id"] for d in drafts_result.data]
            boats_result = (
                self.db.table("boat_schedules")
                .select("id, departure_date")
                .in_("id", other_boat_ids)
                .execute()
            )
            departure_by_boat = {b["id"]: b["departure_date"] for b in boats_result.data}

            # Flag later drafts
            flagged = 0
            now = datetime.now(timezone.utc).isoformat()
            for draft in drafts_result.data:
                draft_departure = departure_by_boat.get(draft["boat_id"], "")
                if draft_departure > this_departure:
                    # Skip ordered/confirmed drafts — they're already committed
                    if draft.get("status") in ("ordered", "confirmed"):
                        continue
                    self.db.table(self.drafts_table).update({
                        "status": "action_needed",
                        "notes": reason,
                        "updated_at": now,
                    }).eq("id", draft["id"]).execute()
                    flagged += 1

            if flagged > 0:
                logger.info(
                    "later_drafts_flagged",
                    boat_id=boat_id,
                    factory_id=factory_id,
                    flagged_count=flagged,
                    reason=reason,
                )
            return flagged

        except Exception as e:
            logger.error("flag_later_drafts_failed", error=str(e))
            return 0  # Non-fatal — don't block the primary operation

    def get_draft(self, boat_id: str, factory_id: str) -> Optional[dict]:
        """
        Get a single draft by boat_id + factory_id.

        Args:
            boat_id: Boat schedule UUID
            factory_id: Factory UUID

        Returns:
            Draft dict with nested items, or None if not found
        """
        logger.info(
            "getting_draft",
            boat_id=boat_id,
            factory_id=factory_id,
        )

        try:
            result = (
                self.db.table(self.drafts_table)
                .select("*")
                .eq("boat_id", boat_id)
                .eq("factory_id", factory_id)
                .execute()
            )

            if not result.data:
                return None

            draft = result.data[0]
            self._attach_items(draft)

            logger.info(
                "draft_retrieved",
                draft_id=draft["id"],
                item_count=len(draft["items"]),
            )
            return draft

        except Exception as e:
            logger.error(
                "get_draft_failed",
                boat_id=boat_id,
                factory_id=factory_id,
                error=str(e),
            )
            raise DatabaseError("select", str(e))

    def list_drafts_for_boat(self, boat_id: str) -> list[dict]:
        """
        Get all drafts for a boat across all factories.

        Args:
            boat_id: Boat schedule UUID

        Returns:
            List of draft dicts, each with nested items
        """
        logger.info("listing_drafts_for_boat", boat_id=boat_id)

        try:
            result = (
                self.db.table(self.drafts_table)
                .select("*")
                .eq("boat_id", boat_id)
                .execute()
            )

            drafts = result.data
            for draft in drafts:
                self._attach_items(draft)

            logger.info(
                "drafts_listed",
                boat_id=boat_id,
                count=len(drafts),
            )
            return drafts

        except Exception as e:
            logger.error(
                "list_drafts_for_boat_failed",
                boat_id=boat_id,
                error=str(e),
            )
            raise DatabaseError("select", str(e))

    def save_draft(
        self,
        boat_id: str,
        factory_id: str,
        notes: Optional[str],
        items: list[dict],
        expected_updated_at: Optional[str] = None,
    ) -> dict:
        """
        Save (create or update) a draft for a boat + factory.

        Upsert logic:
        - If draft exists: update notes + last_edited_at, then replace items
        - If not: insert new draft, then insert items

        Args:
            boat_id: Boat schedule UUID
            factory_id: Factory UUID
            notes: Optional draft-level notes
            items: List of dicts with product_id, selected_pallets, notes

        Returns:
            Saved draft dict with nested items
        """
        logger.info(
            "saving_draft",
            boat_id=boat_id,
            factory_id=factory_id,
            item_count=len(items),
        )

        now = datetime.now(timezone.utc).isoformat()

        try:
            # Check if draft already exists
            existing = (
                self.db.table(self.drafts_table)
                .select("*")
                .eq("boat_id", boat_id)
                .eq("factory_id", factory_id)
                .execute()
            )

            if existing.data:
                current_status = existing.data[0].get("status", "")
                if current_status in ("ordered", "confirmed"):
                    raise DatabaseError(
                        "update",
                        f"Cannot modify draft with status '{current_status}'. Draft is locked after export."
                    )

                # Optimistic locking: reject if updated_at doesn't match (5m)
                if expected_updated_at:
                    current_updated_at = existing.data[0].get("updated_at")
                    if current_updated_at and current_updated_at != expected_updated_at:
                        from exceptions import ConflictError
                        raise ConflictError(
                            code="DRAFT_CONFLICT",
                            message="Este borrador fue modificado por otro usuario. Recarga para ver los cambios.",
                            details={
                                "expected": expected_updated_at,
                                "current": current_updated_at,
                                "draft_id": existing.data[0]["id"],
                            },
                        )

                # Update existing draft
                draft_id = existing.data[0]["id"]

                result = (
                    self.db.table(self.drafts_table)
                    .update({
                        "status": "drafting",
                        "notes": notes,
                        "last_edited_at": now,
                        "updated_at": now,
                    })
                    .eq("id", draft_id)
                    .execute()
                )
                draft = result.data[0]

                logger.info("draft_updated", draft_id=draft_id)

            else:
                # Insert new draft
                result = (
                    self.db.table(self.drafts_table)
                    .insert({
                        "boat_id": boat_id,
                        "factory_id": factory_id,
                        "status": "drafting",
                        "notes": notes,
                        "last_edited_at": now,
                    })
                    .execute()
                )
                draft = result.data[0]
                draft_id = draft["id"]

                logger.info("draft_created", draft_id=draft_id)

            # Save old items for rollback and change detection
            old_items = self._fetch_items(draft_id)
            old_hash = self._compute_items_hash(old_items)

            # Replace items: delete old, insert new
            self.db.table(self.items_table).delete().eq(
                "draft_id", draft_id
            ).execute()

            # Auto-assign BL 1 for unit-based factories (e.g. Muebles — low volume, no container split)
            auto_bl = self._is_unit_based_factory(factory_id)

            if items:
                rows = []
                for item in items:
                    bl = item.get("bl_number")
                    if bl is None and auto_bl:
                        bl = 1
                    item_data = {
                        "draft_id": draft_id,
                        "product_id": item["product_id"],
                        "selected_pallets": item["selected_pallets"],
                        "bl_number": bl,
                        "notes": item.get("notes"),
                    }
                    if item.get("snapshot_data") is not None:
                        item_data["snapshot_data"] = item["snapshot_data"]
                    rows.append(item_data)
                try:
                    self.db.table(self.items_table).insert(rows).execute()
                except Exception as insert_err:
                    # Best-effort rollback: re-insert old items
                    logger.error("draft_items_insert_failed", error=str(insert_err), draft_id=draft_id)
                    if old_items:
                        try:
                            rollback_rows = [{k: v for k, v in item.items() if k != "id"} for item in old_items]
                            self.db.table(self.items_table).insert(rollback_rows).execute()
                        except Exception:
                            logger.error("draft_items_rollback_failed", draft_id=draft_id)
                    raise DatabaseError("insert", f"Failed to save draft items: {insert_err}")

            # Return full draft with items
            self._attach_items(draft)

            # Soft cascade: flag later drafts ONLY if content actually changed
            new_hash = self._compute_items_hash(items)
            if old_hash != new_hash:
                self._flag_later_drafts(boat_id, factory_id, "earlier_draft_modified")
            else:
                logger.debug(
                    "draft_cascade_skipped",
                    draft_id=draft_id,
                    reason="content_hash_unchanged",
                )

            logger.info(
                "draft_saved",
                draft_id=draft_id,
                item_count=len(draft["items"]),
            )
            return draft

        except DatabaseError:
            raise
        except Exception as e:
            from exceptions import AppError
            # Let ConflictError (and other AppErrors) propagate without wrapping
            if isinstance(e, AppError):
                raise
            logger.error(
                "save_draft_failed",
                boat_id=boat_id,
                factory_id=factory_id,
                error=str(e),
            )
            raise DatabaseError("upsert", str(e))

    def _is_unit_based_factory(self, factory_id: str) -> bool:
        """Check if factory is unit-based (e.g. Muebles) vs m2-based (floor tiles)."""
        try:
            result = (
                self.db.table("factories")
                .select("unit_type")
                .eq("id", factory_id)
                .execute()
            )
            if result.data:
                return result.data[0].get("unit_type") == "units"
        except Exception:
            pass
        return False

    def update_status(self, draft_id: str, status: str) -> dict:
        """
        Update the status of a draft.

        If status is "ordered", also sets ordered_at timestamp.

        Args:
            draft_id: Draft UUID
            status: New status (drafting, action_needed, ordered, confirmed)

        Returns:
            Updated draft dict
        """
        logger.info(
            "updating_draft_status",
            draft_id=draft_id,
            status=status,
        )

        try:
            now = datetime.now(timezone.utc).isoformat()
            update_data: dict = {
                "status": status,
                "updated_at": now,
            }

            if status == "ordered":
                update_data["ordered_at"] = now

            result = (
                self.db.table(self.drafts_table)
                .update(update_data)
                .eq("id", draft_id)
                .execute()
            )

            if not result.data:
                logger.warning("draft_not_found_for_status_update", draft_id=draft_id)
                raise DatabaseError(
                    "update",
                    f"Draft {draft_id} not found",
                )

            draft = result.data[0]

            # If transitioning to ordered/confirmed, flag later drafts
            if status in ("ordered", "confirmed"):
                boat_id = draft.get("boat_id")
                factory_id = draft.get("factory_id")
                if boat_id and factory_id:
                    self._flag_later_drafts(
                        boat_id, factory_id, "earlier_draft_ordered"
                    )

            logger.info(
                "draft_status_updated",
                draft_id=draft_id,
                status=status,
            )
            return draft

        except DatabaseError:
            raise
        except Exception as e:
            logger.error(
                "update_draft_status_failed",
                draft_id=draft_id,
                error=str(e),
            )
            raise DatabaseError("update", str(e))

    def delete_draft(self, draft_id: str) -> bool:
        """
        Delete a draft and its items (cascade via FK).
        Flags later drafts as action_needed.

        Args:
            draft_id: Draft UUID

        Returns:
            True if deleted, False if not found
        """
        logger.info("deleting_draft", draft_id=draft_id)

        try:
            # Fetch draft info before deleting (need boat_id + factory_id for cascade)
            draft_result = (
                self.db.table(self.drafts_table)
                .select("boat_id, factory_id")
                .eq("id", draft_id)
                .execute()
            )

            if not draft_result.data:
                logger.warning("draft_not_found_for_delete", draft_id=draft_id)
                return False

            draft_info = draft_result.data[0]

            # Delete the draft
            result = (
                self.db.table(self.drafts_table)
                .delete()
                .eq("id", draft_id)
                .execute()
            )

            if not result.data:
                logger.warning("draft_not_found_for_delete", draft_id=draft_id)
                return False

            logger.info("draft_deleted", draft_id=draft_id)

            # Soft cascade: flag later drafts
            self._flag_later_drafts(
                draft_info["boat_id"],
                draft_info["factory_id"],
                "earlier_draft_deleted",
            )

            return True

        except Exception as e:
            logger.error(
                "delete_draft_failed",
                draft_id=draft_id,
                error=str(e),
            )
            raise DatabaseError("delete", str(e))


# Singleton instance
_service: Optional[DraftService] = None


def get_draft_service() -> DraftService:
    """Get or create DraftService instance."""
    global _service
    if _service is None:
        _service = DraftService()
    return _service

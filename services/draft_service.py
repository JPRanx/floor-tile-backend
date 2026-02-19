"""
Draft service for boat-factory draft management.

Handles CRUD operations for boat_factory_drafts and their draft_items.
Drafts represent product selections being prepared for a factory order on a specific boat.
"""

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
                .select("id, boat_id")
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
                # Update existing draft
                draft_id = existing.data[0]["id"]

                result = (
                    self.db.table(self.drafts_table)
                    .update({
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

            # Replace items: delete old, insert new
            self.db.table(self.items_table).delete().eq(
                "draft_id", draft_id
            ).execute()

            if items:
                rows = []
                for item in items:
                    item_data = {
                        "draft_id": draft_id,
                        "product_id": item["product_id"],
                        "selected_pallets": item["selected_pallets"],
                        "bl_number": item.get("bl_number"),
                        "notes": item.get("notes"),
                    }
                    if item.get("snapshot_data") is not None:
                        item_data["snapshot_data"] = item["snapshot_data"]
                    rows.append(item_data)
                self.db.table(self.items_table).insert(rows).execute()

            # Return full draft with items
            self._attach_items(draft)

            # Soft cascade: flag later drafts
            self._flag_later_drafts(boat_id, factory_id, "Borrador anterior modificado")

            logger.info(
                "draft_saved",
                draft_id=draft_id,
                item_count=len(draft["items"]),
            )
            return draft

        except Exception as e:
            logger.error(
                "save_draft_failed",
                boat_id=boat_id,
                factory_id=factory_id,
                error=str(e),
            )
            raise DatabaseError("upsert", str(e))

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
                "Borrador anterior eliminado",
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

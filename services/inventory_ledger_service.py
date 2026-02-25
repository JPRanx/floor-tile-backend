"""
Inventory Ledger Service - event-sourced inventory tracking.

Records inventory events (deltas and reconciliations) and maintains
projected state per product. The ledger is append-only; projected
state is derived by applying events to reconciliation baselines.

See STANDARDS_LOGGING.md for logging patterns.
See STANDARDS_ERRORS.md for error handling patterns.
"""

from typing import Optional
from decimal import Decimal
from datetime import date, datetime, timezone

import structlog

from config import get_supabase_client

logger = structlog.get_logger(__name__)


class InventoryLedgerService:
    """
    Inventory ledger business logic.

    Core methods:
    - record_event: Append a delta or reconciliation event
    - reconcile_*: Snap projected state to truth from uploads
    - record_sales_batch: Batch deductions from sales upload
    - record_*_exported: UI actions from Order Builder
    - generate_reconciliation_report: Audit trail for reconciliations
    - get_*: Query methods for UI
    """

    def __init__(self):
        self.db = get_supabase_client()

    def _is_enabled(self) -> bool:
        """Check if ledger is enabled via settings."""
        try:
            result = (
                self.db.table("settings")
                .select("value")
                .eq("key", "ledger_enabled")
                .single()
                .execute()
            )
            return result.data is not None and result.data.get("value") == "true"
        except Exception:
            return False

    def record_event(
        self,
        event_type: str,
        product_id: str,
        delta_warehouse_m2: Decimal = Decimal("0"),
        delta_factory_m2: Decimal = Decimal("0"),
        delta_transit_m2: Decimal = Decimal("0"),
        snapshot_value_m2: Optional[Decimal] = None,
        source_type: str = "system",
        source_id: Optional[str] = None,
        source_filename: Optional[str] = None,
        event_date: Optional[date] = None,
        notes: Optional[str] = None,
    ) -> dict:
        """
        Core event recording method.

        Algorithm:
        1. Ensure inventory_projected row exists for product (upsert default 0s)
        2. Read current projected state
        3. For RECONCILED events:
           a. projected_value = current state for that bucket
           b. discrepancy = snapshot_value - projected_value
           c. delta = snapshot_value - projected_value (snap to truth)
        4. INSERT into inventory_ledger
        5. UPDATE inventory_projected:
           - RECONCILED: SET bucket = snapshot_value, reconciled_at = now(), events_since = 0
           - DELTA: SET bucket += delta, events_since += 1
        """
        if not self._is_enabled():
            return {}

        event_date = event_date or date.today()

        # 1. Ensure projected row exists
        self.db.table("inventory_projected").upsert(
            {"product_id": product_id},
            on_conflict="product_id",
        ).execute()

        # 2. Read current state
        state_result = (
            self.db.table("inventory_projected")
            .select("*")
            .eq("product_id", product_id)
            .single()
            .execute()
        )
        state = state_result.data

        # 3. Handle reconciliation events
        is_reconciliation = event_type.endswith("_RECONCILED")
        projected_value_m2 = None
        discrepancy_m2 = None

        if is_reconciliation and snapshot_value_m2 is not None:
            bucket_map = {
                "WAREHOUSE_RECONCILED": "warehouse_m2",
                "FACTORY_RECONCILED": "factory_m2",
                "TRANSIT_RECONCILED": "transit_m2",
            }
            bucket = bucket_map.get(event_type)
            if bucket:
                projected_value_m2 = Decimal(str(state.get(bucket, 0)))
                discrepancy_m2 = snapshot_value_m2 - projected_value_m2
                # Override the delta to snap to truth
                if bucket == "warehouse_m2":
                    delta_warehouse_m2 = discrepancy_m2
                elif bucket == "factory_m2":
                    delta_factory_m2 = discrepancy_m2
                elif bucket == "transit_m2":
                    delta_transit_m2 = discrepancy_m2

        # 4. INSERT event
        event_row = {
            "event_type": event_type,
            "product_id": product_id,
            "delta_warehouse_m2": str(delta_warehouse_m2),
            "delta_factory_m2": str(delta_factory_m2),
            "delta_transit_m2": str(delta_transit_m2),
            "snapshot_value_m2": str(snapshot_value_m2) if snapshot_value_m2 is not None else None,
            "projected_value_m2": str(projected_value_m2) if projected_value_m2 is not None else None,
            "discrepancy_m2": str(discrepancy_m2) if discrepancy_m2 is not None else None,
            "source_type": source_type,
            "source_id": source_id,
            "source_filename": source_filename,
            "event_date": event_date.isoformat(),
            "notes": notes,
        }
        insert_result = self.db.table("inventory_ledger").insert(event_row).execute()
        event = insert_result.data[0] if insert_result.data else {}

        logger.info(
            "ledger_event_recorded",
            event_type=event_type,
            product_id=product_id,
            event_id=event.get("id"),
        )

        # 5. UPDATE projected state
        now_ts = datetime.now(timezone.utc).isoformat()

        if is_reconciliation and snapshot_value_m2 is not None:
            recon_update: dict = {}
            if event_type == "WAREHOUSE_RECONCILED":
                recon_update = {
                    "warehouse_m2": str(snapshot_value_m2),
                    "warehouse_reconciled_at": now_ts,
                    "events_since_warehouse_recon": 0,
                }
            elif event_type == "FACTORY_RECONCILED":
                recon_update = {
                    "factory_m2": str(snapshot_value_m2),
                    "factory_reconciled_at": now_ts,
                    "events_since_factory_recon": 0,
                }
            elif event_type == "TRANSIT_RECONCILED":
                recon_update = {
                    "transit_m2": str(snapshot_value_m2),
                    "transit_reconciled_at": now_ts,
                    "events_since_transit_recon": 0,
                }

            if recon_update:
                recon_update["last_event_id"] = event.get("id")
                recon_update["last_event_at"] = now_ts
                recon_update["updated_at"] = now_ts
                self.db.table("inventory_projected").update(recon_update).eq(
                    "product_id", product_id
                ).execute()
        else:
            # Delta update: read-then-write
            # Acceptable since ledger is append-only and projected is single-writer
            new_warehouse = Decimal(str(state.get("warehouse_m2", 0))) + delta_warehouse_m2
            new_factory = Decimal(str(state.get("factory_m2", 0))) + delta_factory_m2
            new_transit = Decimal(str(state.get("transit_m2", 0))) + delta_transit_m2

            delta_update = {
                "warehouse_m2": str(new_warehouse),
                "factory_m2": str(new_factory),
                "transit_m2": str(new_transit),
                "events_since_warehouse_recon": state.get("events_since_warehouse_recon", 0)
                + (1 if delta_warehouse_m2 != 0 else 0),
                "events_since_factory_recon": state.get("events_since_factory_recon", 0)
                + (1 if delta_factory_m2 != 0 else 0),
                "events_since_transit_recon": state.get("events_since_transit_recon", 0)
                + (1 if delta_transit_m2 != 0 else 0),
                "last_event_id": event.get("id"),
                "last_event_at": now_ts,
                "updated_at": now_ts,
            }
            self.db.table("inventory_projected").update(delta_update).eq(
                "product_id", product_id
            ).execute()

        return event

    # ===================
    # UPLOAD DELTA METHODS
    # ===================

    def record_sales_batch(
        self,
        items: list,
        source_filename: Optional[str] = None,
    ) -> int:
        """
        Record batch of sales as warehouse deductions.

        Args:
            items: [{"product_id": str, "quantity_m2": Decimal, "event_date": date}]
            source_filename: Upload filename for audit trail

        Returns:
            Count of events recorded.
        """
        if not self._is_enabled():
            return 0

        count = 0
        for item in items:
            self.record_event(
                event_type="SALE_RECORDED",
                product_id=item["product_id"],
                delta_warehouse_m2=-abs(Decimal(str(item["quantity_m2"]))),
                source_type="upload",
                source_filename=source_filename,
                event_date=item.get("event_date"),
            )
            count += 1

        logger.info(
            "sales_batch_recorded",
            count=count,
            source_filename=source_filename,
        )
        return count

    # ===================
    # UI DELTA METHODS (Order Builder exports)
    # ===================

    def record_warehouse_order_exported(
        self,
        product_id: str,
        ordered_m2: Decimal,
        source_id: Optional[str] = None,
    ) -> dict:
        """Section 1: Export Warehouse Order -- factory -= ordered (SIESA stock committed)."""
        return self.record_event(
            event_type="WAREHOUSE_ORDER_EXPORTED",
            product_id=product_id,
            delta_factory_m2=-abs(ordered_m2),
            source_type="ui",
            source_id=source_id,
        )

    def record_piggyback_confirmed(
        self,
        product_id: str,
        added_m2: Decimal,
        source_id: Optional[str] = None,
    ) -> dict:
        """Section 2: Confirm Piggyback -- production += added."""
        return self.record_event(
            event_type="PIGGYBACK_CONFIRMED",
            product_id=product_id,
            source_type="ui",
            source_id=source_id,
            notes=f"Piggyback: +{added_m2} m2",
        )

    def record_factory_order_exported(
        self,
        product_id: str,
        requested_m2: Decimal,
        source_id: Optional[str] = None,
    ) -> dict:
        """Section 3: Export Factory Order -- production += requested."""
        return self.record_event(
            event_type="FACTORY_ORDER_EXPORTED",
            product_id=product_id,
            source_type="ui",
            source_id=source_id,
            notes=f"Factory order: +{requested_m2} m2",
        )

    # ===================
    # RECONCILIATION METHODS
    # ===================

    def reconcile_warehouse(
        self,
        product_id: str,
        actual_m2: Decimal,
        source_filename: Optional[str] = None,
    ) -> dict:
        """Warehouse upload -- snap warehouse to truth."""
        return self.record_event(
            event_type="WAREHOUSE_RECONCILED",
            product_id=product_id,
            snapshot_value_m2=actual_m2,
            source_type="upload",
            source_filename=source_filename,
        )

    def reconcile_factory(
        self,
        product_id: str,
        actual_m2: Decimal,
        source_filename: Optional[str] = None,
    ) -> dict:
        """SIESA upload -- snap factory to truth."""
        return self.record_event(
            event_type="FACTORY_RECONCILED",
            product_id=product_id,
            snapshot_value_m2=actual_m2,
            source_type="upload",
            source_filename=source_filename,
        )

    def reconcile_transit(
        self,
        product_id: str,
        actual_m2: Decimal,
        source_filename: Optional[str] = None,
    ) -> dict:
        """In-transit upload -- snap transit to truth."""
        return self.record_event(
            event_type="TRANSIT_RECONCILED",
            product_id=product_id,
            snapshot_value_m2=actual_m2,
            source_type="upload",
            source_filename=source_filename,
        )

    def reconcile_production(
        self,
        product_id: str,
        actual_m2: Decimal,
        source_filename: Optional[str] = None,
    ) -> dict:
        """Production upload -- compare against requests."""
        return self.record_event(
            event_type="PRODUCTION_RECONCILED",
            product_id=product_id,
            snapshot_value_m2=actual_m2,
            source_type="upload",
            source_filename=source_filename,
        )

    # ===================
    # REPORTS
    # ===================

    def generate_reconciliation_report(
        self,
        recon_type: str,
        items: list,
        filename: Optional[str] = None,
    ) -> dict:
        """
        Generate and store a reconciliation report.

        Args:
            recon_type: Type of reconciliation (warehouse, factory, transit, production)
            items: [{"product_id": str, "projected_m2": Decimal, "actual_m2": Decimal, "discrepancy_m2": Decimal}]
            filename: Source filename for audit trail

        Returns:
            Created report dict, or empty dict if ledger disabled.
        """
        if not self._is_enabled():
            return {}

        matched = sum(1 for i in items if Decimal(str(i.get("discrepancy_m2", 0))) == 0)
        discrepant = len(items) - matched
        total_proj = sum(Decimal(str(i.get("projected_m2", 0))) for i in items)
        total_actual = sum(Decimal(str(i.get("actual_m2", 0))) for i in items)
        total_disc = sum(Decimal(str(i.get("discrepancy_m2", 0))) for i in items)

        report_row = {
            "reconciliation_type": recon_type,
            "filename": filename,
            "reconciliation_date": date.today().isoformat(),
            "products_reconciled": len(items),
            "products_matched": matched,
            "products_discrepant": discrepant,
            "total_projected_m2": str(total_proj),
            "total_actual_m2": str(total_actual),
            "total_discrepancy_m2": str(total_disc),
            "items": items,
        }

        result = self.db.table("reconciliation_reports").insert(report_row).execute()

        logger.info(
            "reconciliation_report_generated",
            recon_type=recon_type,
            products_reconciled=len(items),
            products_matched=matched,
            products_discrepant=discrepant,
        )

        return result.data[0] if result.data else {}

    # ===================
    # QUERY METHODS
    # ===================

    def get_projected_state(self, product_id: str) -> Optional[dict]:
        """Get projected state for a single product."""
        try:
            result = (
                self.db.table("inventory_projected")
                .select("*")
                .eq("product_id", product_id)
                .single()
                .execute()
            )
            return result.data
        except Exception:
            return None

    def get_all_projected(self) -> list:
        """Get all projected states."""
        result = self.db.table("inventory_projected").select("*").execute()
        return result.data or []

    def get_events(
        self,
        product_id: Optional[str] = None,
        event_type: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 100,
    ) -> list:
        """Get ledger events with optional filters."""
        query = (
            self.db.table("inventory_ledger")
            .select("*")
            .order("created_at", desc=True)
            .limit(limit)
        )
        if product_id:
            query = query.eq("product_id", product_id)
        if event_type:
            query = query.eq("event_type", event_type)
        if since:
            query = query.gte("created_at", since)
        result = query.execute()
        return result.data or []

    def get_reconciliation_reports(
        self,
        recon_type: Optional[str] = None,
        limit: int = 20,
    ) -> list:
        """Get reconciliation reports."""
        query = (
            self.db.table("reconciliation_reports")
            .select("*")
            .order("created_at", desc=True)
            .limit(limit)
        )
        if recon_type:
            query = query.eq("reconciliation_type", recon_type)
        result = query.execute()
        return result.data or []


# ===================
# SINGLETON
# ===================

_ledger_service: Optional[InventoryLedgerService] = None


def get_ledger_service() -> InventoryLedgerService:
    """Get or create InventoryLedgerService instance."""
    global _ledger_service
    if _ledger_service is None:
        _ledger_service = InventoryLedgerService()
    return _ledger_service

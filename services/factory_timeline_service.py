"""
Factory timeline service for Order Builder V2.

Computes factory-specific timeline milestones for a given factory + boat
combination, enabling the "You Are Here" timeline feature in Planning View.
"""

from datetime import date, timedelta
from typing import Optional
import structlog

from config.shipping import WAREHOUSE_BUFFER_DAYS

logger = structlog.get_logger(__name__)

# Days of the work week mapped to Python weekday integers
WEEKDAY_MAP = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
}

# Buffer days subtracted when computing factory_request_cutoff
FACTORY_REQUEST_BUFFER_DAYS = 5

# Buffer days subtracted when computing order_deadline
SHIPPING_BUFFER_DAYS = 3

# Human-readable labels for each milestone
MILESTONE_LABELS = {
    "factory_request_cutoff": "Submit factory production request",
    "piggyback_cutoff": "Add to scheduled production",
    "order_deadline": "Finalize order for this boat",
    "departure_date": "Boat departs",
    "arrival_date": "Boat arrives at port",
    "in_warehouse_date": "Goods in warehouse",
}

# Ordered list of milestone keys (chronological)
MILESTONE_ORDER = [
    "factory_request_cutoff",
    "piggyback_cutoff",
    "order_deadline",
    "departure_date",
    "arrival_date",
    "in_warehouse_date",
]


class FactoryTimelineService:
    """
    Computes timeline milestones for a factory + boat combination.

    Each milestone represents a key date in the ordering/shipping workflow.
    The service also identifies which milestone is "next" so the UI can
    show Ashley where she is in the process.
    """

    def compute_milestones(
        self,
        factory: dict,
        departure_date: date,
        arrival_date: date,
        has_scheduled_production: bool = False,
        today: Optional[date] = None,
    ) -> dict:
        """
        Compute the 6 timeline milestones for a factory + boat combination.

        Args:
            factory: Factory dict with production_lead_days, transport_to_port_days,
                     cutoff_day (e.g. "monday")
            departure_date: Boat departure date
            arrival_date: Boat arrival date
            has_scheduled_production: Whether factory has items in production
                for this period
            today: Override for current date (useful for testing)

        Returns:
            Dict with milestone dates, current_milestone indicator,
            days_to_next_milestone, and ordered milestones list
        """
        if today is None:
            today = date.today()

        production_lead_days = factory.get("production_lead_days", 0)
        transport_to_port_days = factory.get("transport_to_port_days", 0)

        # --- Milestone 1: Factory Request Cutoff ---
        factory_request_cutoff = (
            departure_date
            - timedelta(days=production_lead_days)
            - timedelta(days=transport_to_port_days)
            - timedelta(days=FACTORY_REQUEST_BUFFER_DAYS)
        )

        # --- Milestone 2: Piggyback Cutoff ---
        piggyback_cutoff = self._compute_piggyback_cutoff(
            factory=factory,
            departure_date=departure_date,
            has_scheduled_production=has_scheduled_production,
            today=today,
        )

        # --- Milestone 3: Order Deadline ---
        order_deadline = (
            departure_date
            - timedelta(days=transport_to_port_days)
            - timedelta(days=SHIPPING_BUFFER_DAYS)
        )

        # --- Milestone 4 & 5: Pass-through ---
        # departure_date and arrival_date are used directly

        # --- Milestone 6: In Warehouse ---
        in_warehouse_date = arrival_date + timedelta(days=WAREHOUSE_BUFFER_DAYS)

        # Build the dates mapping (None for cutoffs that don't apply)
        milestone_dates = {
            "factory_request_cutoff": factory_request_cutoff,
            "piggyback_cutoff": piggyback_cutoff,
            "order_deadline": order_deadline,
            "departure_date": departure_date,
            "arrival_date": arrival_date,
            "in_warehouse_date": in_warehouse_date,
        }

        # Determine current milestone and days until it
        current_milestone, days_to_next = self._find_current_milestone(
            milestone_dates, today
        )

        # Build the label for the current milestone
        current_milestone_label = None
        if current_milestone is not None:
            current_milestone_label = MILESTONE_LABELS[current_milestone]

        # Build ordered milestones list for timeline rendering
        milestones_list = self._build_milestones_list(milestone_dates, today)

        logger.debug(
            "milestones_computed",
            factory_name=factory.get("name"),
            departure_date=str(departure_date),
            current_milestone=current_milestone,
            days_to_next=days_to_next,
        )

        return {
            "factory_request_cutoff": (
                factory_request_cutoff.isoformat()
                if factory_request_cutoff
                else None
            ),
            "piggyback_cutoff": (
                piggyback_cutoff.isoformat() if piggyback_cutoff else None
            ),
            "order_deadline": order_deadline.isoformat(),
            "departure_date": departure_date.isoformat(),
            "arrival_date": arrival_date.isoformat(),
            "in_warehouse_date": in_warehouse_date.isoformat(),
            "current_milestone": current_milestone,
            "current_milestone_label": current_milestone_label,
            "days_to_next_milestone": days_to_next,
            "milestones": milestones_list,
        }

    def _compute_piggyback_cutoff(
        self,
        factory: dict,
        departure_date: date,
        has_scheduled_production: bool,
        today: date,
    ) -> Optional[date]:
        """
        Compute the next piggyback cutoff date.

        The piggyback cutoff is the next occurrence of the factory's cutoff_day
        after today. Only relevant if there is scheduled production to add to.

        Returns:
            The next cutoff date, or None if not applicable.
        """
        if not has_scheduled_production:
            return None

        cutoff_day = factory.get("cutoff_day")
        if not cutoff_day or cutoff_day.lower() not in WEEKDAY_MAP:
            return None

        target_weekday = WEEKDAY_MAP[cutoff_day.lower()]
        days_ahead = (target_weekday - today.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7  # next week if today IS the cutoff day

        next_cutoff = today + timedelta(days=days_ahead)

        # Only valid if before the departure date
        if next_cutoff >= departure_date:
            return None

        return next_cutoff

    def _find_current_milestone(
        self,
        milestone_dates: dict,
        today: date,
    ) -> tuple[Optional[str], Optional[int]]:
        """
        Find the next upcoming milestone after today.

        Args:
            milestone_dates: Dict of milestone key -> date (or None)
            today: Current date

        Returns:
            Tuple of (milestone_key, days_until) or (None, None) if all passed
        """
        for key in MILESTONE_ORDER:
            milestone_date = milestone_dates.get(key)
            if milestone_date is None:
                continue
            if milestone_date >= today:
                days_until = (milestone_date - today).days
                return key, days_until

        # All milestones have passed
        return None, None

    def _build_milestones_list(
        self,
        milestone_dates: dict,
        today: date,
    ) -> list[dict]:
        """
        Build the ordered milestones list for timeline rendering.

        Args:
            milestone_dates: Dict of milestone key -> date (or None)
            today: Current date

        Returns:
            List of milestone dicts with key, label, date, and passed status
        """
        milestones = []
        for key in MILESTONE_ORDER:
            milestone_date = milestone_dates.get(key)
            if milestone_date is None:
                continue
            milestones.append({
                "key": key,
                "label": MILESTONE_LABELS[key],
                "date": milestone_date.isoformat(),
                "passed": milestone_date < today,
            })
        return milestones


# Singleton instance
_service: Optional[FactoryTimelineService] = None


def get_factory_timeline_service() -> FactoryTimelineService:
    """Get or create FactoryTimelineService instance."""
    global _service
    if _service is None:
        _service = FactoryTimelineService()
    return _service

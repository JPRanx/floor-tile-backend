"""
Clean V1 configuration knobs (accepted spec §6.1/§14).

Every knob here is NEW configuration wiring (not covered by the seven
get_shipping_config() keys). `default_voyage_days` deliberately has NO
default value: U3 is CLOSED — no verified ocean-voyage value exists in the
snapshot and no real operating default may be chosen in this tranche. It is
injected per environment; the §13 acceptance fixtures inject the synthetic
value 15 (a fixture value only, NOT a product decision).
"""

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass(frozen=True)
class PlanningConfig:
    # U3 — REQUIRED injection; no real default exists (gate CLOSED)
    default_voyage_days: int

    # §10.2 — doctrine range 25–30%, knob default 25% (U5, non-blocking)
    material_variance_pct: Decimal = Decimal("0.25")

    # §6.3 — deterministic next-cycle order day, allowed 20–25, default 25 (U6)
    next_cycle_order_day: int = 25

    # §10.3 — awaiting-factory overdue window, default 7 days (U6)
    factory_response_overdue_days: int = 7

    # C6/S9 — critical-feed freshness thresholds (days)
    freshness_aging_days: int = 7
    freshness_stale_days: int = 30

    # Reconciled V1 §9.3 windows (U-R2 — no verified source values exist;
    # config knobs flagged for operator confirmation, mirroring U6)
    commitment_observation_days: int = 3
    booking_overdue_days: int = 2
    transit_evidence_days: int = 5

    def __post_init__(self):
        if not (20 <= self.next_cycle_order_day <= 25):
            raise ValueError("next_cycle_order_day must be within 20–25 (§6.3)")
        if self.default_voyage_days < 0:
            raise ValueError("default_voyage_days must be ≥ 0")
        if not (Decimal("0") < self.material_variance_pct < Decimal("1")):
            raise ValueError("material_variance_pct must be a fraction in (0, 1)")

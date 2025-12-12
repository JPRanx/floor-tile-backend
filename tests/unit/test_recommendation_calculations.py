"""
Unit tests for core recommendation calculations.

Tests the critical calculation functions before production push:
1. Coverage Gap Calculation
2. Confidence Calculation
3. Customer Name Normalization
4. Priority Thresholds
"""

import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch

from services.recommendation_service import (
    RecommendationService,
    M2_PER_PALLET,
)
from models.recommendation import (
    ConfidenceLevel,
    ActionType,
    RecommendationPriority,
)
from services.stockout_service import StockoutStatus
from utils.text_utils import normalize_customer_name


# ===================
# FIXTURES
# ===================

@pytest.fixture
def mock_services():
    """Mock all external service dependencies."""
    with patch("services.recommendation_service.get_product_service") as prod, \
         patch("services.recommendation_service.get_inventory_service") as inv, \
         patch("services.recommendation_service.get_sales_service") as sales, \
         patch("services.recommendation_service.get_stockout_service") as stock, \
         patch("services.recommendation_service.get_boat_schedule_service") as boat, \
         patch("services.recommendation_service.settings") as settings:

        settings.lead_time_days = 45
        settings.warehouse_max_pallets = 740
        settings.velocity_window_weeks = 12
        settings.low_volume_min_records = 2

        prod.return_value = MagicMock()
        inv.return_value = MagicMock()
        sales.return_value = MagicMock()
        stock.return_value = MagicMock()
        boat.return_value = MagicMock()
        boat.return_value.get_next_two_arrivals.return_value = (None, None)

        yield {
            "product": prod,
            "inventory": inv,
            "sales": sales,
            "stockout": stock,
            "boat": boat,
            "settings": settings,
        }


@pytest.fixture
def recommendation_service(mock_services):
    """Create RecommendationService with mocked dependencies."""
    return RecommendationService()


# ===================
# TEST 1: COVERAGE GAP CALCULATION
# ===================

class TestCoverageGapCalculation:
    """
    Tests for _calculate_coverage_gap method.

    Formula: coverage_gap = (daily_velocity × days_to_cover) - available_stock
    """

    def test_positive_coverage_gap_needs_order(self, recommendation_service):
        """
        Positive gap means we need to order.

        daily_velocity = 100 m²/day
        days_to_cover = 30 days
        available = 1000 m²
        total_demand = 100 × 30 = 3000 m²
        gap = 3000 - 1000 = 2000 m² (positive = need to order)
        pallets = ceil(2000/135) = 15 pallets
        """
        total_demand, gap_m2, gap_pallets = recommendation_service._calculate_coverage_gap(
            daily_velocity=Decimal("100"),
            available_m2=Decimal("1000"),
            days_to_cover=30,
        )

        assert total_demand == Decimal("3000.00")
        assert gap_m2 == Decimal("2000.00")
        assert gap_pallets == 15  # ceil(2000/135) = 15

    def test_negative_coverage_gap_has_buffer(self, recommendation_service):
        """
        Negative gap means we have enough stock (buffer).

        daily_velocity = 50 m²/day
        days_to_cover = 30 days
        available = 2000 m²
        total_demand = 50 × 30 = 1500 m²
        gap = 1500 - 2000 = -500 m² (negative = have buffer)
        pallets = 0 (no order needed)
        """
        total_demand, gap_m2, gap_pallets = recommendation_service._calculate_coverage_gap(
            daily_velocity=Decimal("50"),
            available_m2=Decimal("2000"),
            days_to_cover=30,
        )

        assert total_demand == Decimal("1500.00")
        assert gap_m2 == Decimal("-500.00")
        assert gap_pallets == 0  # Negative gap = 0 pallets needed

    def test_coverage_gap_with_no_boat_uses_lead_time(self, recommendation_service):
        """
        When no boat arrival scheduled, days_to_cover defaults to lead_time.

        With lead_time=45 days:
        daily_velocity = 80 m²/day
        days_to_cover = 45 days
        available = 1000 m²
        total_demand = 80 × 45 = 3600 m²
        gap = 3600 - 1000 = 2600 m²
        pallets = ceil(2600/135) = 20 pallets
        """
        total_demand, gap_m2, gap_pallets = recommendation_service._calculate_coverage_gap(
            daily_velocity=Decimal("80"),
            available_m2=Decimal("1000"),
            days_to_cover=45,  # Fallback to lead_time
        )

        assert total_demand == Decimal("3600.00")
        assert gap_m2 == Decimal("2600.00")
        assert gap_pallets == 20  # ceil(2600/135) = 20

    def test_coverage_gap_zero_velocity(self, recommendation_service):
        """Zero velocity means zero demand, negative gap from existing stock."""
        total_demand, gap_m2, gap_pallets = recommendation_service._calculate_coverage_gap(
            daily_velocity=Decimal("0"),
            available_m2=Decimal("500"),
            days_to_cover=30,
        )

        assert total_demand == Decimal("0.00")
        assert gap_m2 == Decimal("-500.00")
        assert gap_pallets == 0

    def test_coverage_gap_exact_match(self, recommendation_service):
        """When demand exactly matches available stock."""
        total_demand, gap_m2, gap_pallets = recommendation_service._calculate_coverage_gap(
            daily_velocity=Decimal("100"),
            available_m2=Decimal("3000"),
            days_to_cover=30,
        )

        assert total_demand == Decimal("3000.00")
        assert gap_m2 == Decimal("0.00")
        assert gap_pallets == 0


# ===================
# TEST 2: CONFIDENCE CALCULATION
# ===================

class TestConfidenceCalculation:
    """
    Tests for _calculate_confidence method.

    Tests the 10 confidence rules based on customer data and sales patterns.
    """

    def test_high_confidence_diverse_customers(self, recommendation_service):
        """
        HIGH confidence when:
        - 4+ weeks of data
        - Recent sales activity
        - 3+ unique customers
        - No single customer dominates
        """
        weekly_sales = [Decimal("500"), Decimal("600"), Decimal("550"), Decimal("580")]
        customer_analysis = {
            "unique_customers": 5,
            "top_customer_name": "Customer A",
            "top_customer_share": Decimal("0.25"),  # 25% - not dominant
            "recurring_count": 3,
            "recurring_share": Decimal("0.75"),  # 75% from recurring
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=weekly_sales,
            weeks_of_data=4,
            customer_analysis=customer_analysis,
        )

        assert confidence == ConfidenceLevel.HIGH
        assert "customers" in reason.lower()

    def test_low_confidence_single_customer(self, recommendation_service):
        """
        LOW confidence when only 1 unique customer.
        Even with good history, single customer dependency is risky.
        """
        weekly_sales = [Decimal("1000"), Decimal("1000"), Decimal("1000"), Decimal("1000")]
        customer_analysis = {
            "unique_customers": 1,
            "top_customer_name": "Big Corp",
            "top_customer_share": Decimal("1.0"),  # 100%
            "recurring_count": 1,
            "recurring_share": Decimal("1.0"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=weekly_sales,
            weeks_of_data=8,
            customer_analysis=customer_analysis,
        )

        assert confidence == ConfidenceLevel.LOW
        assert "single customer" in reason.lower() or "big corp" in reason.lower()

    def test_low_confidence_high_concentration(self, recommendation_service):
        """
        LOW confidence when top customer has >70% of sales.
        High concentration = high dependency risk.
        """
        weekly_sales = [Decimal("800"), Decimal("850"), Decimal("820"), Decimal("830")]
        customer_analysis = {
            "unique_customers": 4,
            "top_customer_name": "Major Client",
            "top_customer_share": Decimal("0.75"),  # 75% - high concentration
            "recurring_count": 2,
            "recurring_share": Decimal("0.80"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=weekly_sales,
            weeks_of_data=4,
            customer_analysis=customer_analysis,
        )

        assert confidence == ConfidenceLevel.LOW
        assert "75%" in reason or "major client" in reason.lower()

    def test_medium_confidence_moderate_concentration(self, recommendation_service):
        """
        MEDIUM confidence when top customer has 50-70% of sales.
        """
        weekly_sales = [Decimal("500"), Decimal("600"), Decimal("550"), Decimal("580")]
        customer_analysis = {
            "unique_customers": 3,
            "top_customer_name": "Regular Customer",
            "top_customer_share": Decimal("0.55"),  # 55% - moderate concentration
            "recurring_count": 2,
            "recurring_share": Decimal("0.70"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=weekly_sales,
            weeks_of_data=4,
            customer_analysis=customer_analysis,
        )

        assert confidence == ConfidenceLevel.MEDIUM
        assert "55%" in reason or "regular customer" in reason.lower()

    def test_low_confidence_no_recent_sales(self, recommendation_service):
        """
        LOW confidence when no sales in last 4 weeks.
        """
        # Sales in early weeks, none recently
        weekly_sales = [Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"),
                       Decimal("500"), Decimal("600"), Decimal("550"), Decimal("580")]
        customer_analysis = {
            "unique_customers": 5,
            "top_customer_name": "Customer A",
            "top_customer_share": Decimal("0.20"),
            "recurring_count": 3,
            "recurring_share": Decimal("0.75"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=weekly_sales,
            weeks_of_data=8,
            customer_analysis=customer_analysis,
        )

        assert confidence == ConfidenceLevel.LOW
        assert "no sales" in reason.lower() or "4 weeks" in reason.lower()

    def test_low_confidence_few_weeks_of_data(self, recommendation_service):
        """
        LOW confidence when less than 4 weeks of data.
        """
        weekly_sales = [Decimal("500"), Decimal("600")]  # Only 2 weeks
        customer_analysis = {
            "unique_customers": 3,
            "top_customer_name": "Customer A",
            "top_customer_share": Decimal("0.30"),
            "recurring_count": 2,
            "recurring_share": Decimal("0.70"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=weekly_sales,
            weeks_of_data=2,
            customer_analysis=customer_analysis,
        )

        assert confidence == ConfidenceLevel.LOW
        assert "2 weeks" in reason

    def test_low_confidence_no_data(self, recommendation_service):
        """LOW confidence when no sales data at all."""
        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=[],
            weeks_of_data=0,
            customer_analysis=None,
        )

        assert confidence == ConfidenceLevel.LOW
        assert "no sales data" in reason.lower()

    def test_medium_confidence_few_customers(self, recommendation_service):
        """
        MEDIUM confidence when only 2 customers (not single, but not diverse).
        """
        weekly_sales = [Decimal("500"), Decimal("600"), Decimal("550"), Decimal("580")]
        customer_analysis = {
            "unique_customers": 2,
            "top_customer_name": "Customer A",
            "top_customer_share": Decimal("0.45"),  # Under 50%
            "recurring_count": 2,
            "recurring_share": Decimal("0.90"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=weekly_sales,
            weeks_of_data=4,
            customer_analysis=customer_analysis,
        )

        assert confidence == ConfidenceLevel.MEDIUM
        assert "2 customers" in reason.lower()

    def test_low_confidence_erratic_sales(self, recommendation_service):
        """
        LOW confidence when sales pattern is highly erratic (CV > 0.8).
        """
        # Highly variable sales - CV must be > 0.8
        # Values: 50, 1000, 30, 900, 100, 950, 20, 1000
        # Mean ≈ 506, StdDev ≈ 458, CV ≈ 0.90 > 0.8
        weekly_sales = [Decimal("50"), Decimal("1000"), Decimal("30"), Decimal("900"),
                       Decimal("100"), Decimal("950"), Decimal("20"), Decimal("1000")]
        customer_analysis = {
            "unique_customers": 5,
            "top_customer_name": "Customer A",
            "top_customer_share": Decimal("0.20"),
            "recurring_count": 3,
            "recurring_share": Decimal("0.60"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=weekly_sales,
            weeks_of_data=8,
            customer_analysis=customer_analysis,
        )

        assert confidence == ConfidenceLevel.LOW
        assert "erratic" in reason.lower()

    def test_customer_metrics_returned(self, recommendation_service):
        """Verify customer metrics are properly returned."""
        weekly_sales = [Decimal("500"), Decimal("600"), Decimal("550"), Decimal("580")]
        customer_analysis = {
            "unique_customers": 5,
            "top_customer_name": "Test Corp",
            "top_customer_share": Decimal("0.30"),
            "recurring_count": 3,
            "recurring_share": Decimal("0.75"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=weekly_sales,
            weeks_of_data=4,
            customer_analysis=customer_analysis,
        )

        assert metrics["unique_customers"] == 5
        assert metrics["top_customer_name"] == "Test Corp"
        assert metrics["top_customer_share"] == Decimal("0.30")
        assert metrics["recurring_customers"] == 3
        assert metrics["recurring_share"] == Decimal("0.75")


# ===================
# TEST 3: CUSTOMER NAME NORMALIZATION
# ===================

class TestCustomerNameNormalization:
    """
    Tests for normalize_customer_name function.

    Handles Spanish accents and special characters for consistent grouping.
    """

    def test_accent_removal(self):
        """Spanish accents removed: Decoración → DECORACION."""
        assert normalize_customer_name("Decoración García") == "DECORACION GARCIA"
        assert normalize_customer_name("José") == "JOSE"
        assert normalize_customer_name("María") == "MARIA"
        assert normalize_customer_name("Señor López") == "SENOR LOPEZ"

    def test_whitespace_handling(self):
        """Leading/trailing whitespace stripped."""
        assert normalize_customer_name("  PISOS S.A.  ") == "PISOS S.A."
        assert normalize_customer_name("\tClient Corp\n") == "CLIENT CORP"
        assert normalize_customer_name("   ") is None

    def test_case_normalization(self):
        """Converts to uppercase."""
        assert normalize_customer_name("pisos del norte") == "PISOS DEL NORTE"
        assert normalize_customer_name("PiSoS DeL NoRtE") == "PISOS DEL NORTE"
        assert normalize_customer_name("ALREADY UPPER") == "ALREADY UPPER"

    def test_special_characters_preserved(self):
        """Non-accent special characters preserved."""
        assert normalize_customer_name("José's Tiles") == "JOSE'S TILES"
        assert normalize_customer_name("Corp. Inc.") == "CORP. INC."
        assert normalize_customer_name("A & B Partners") == "A & B PARTNERS"

    def test_empty_and_none_handling(self):
        """Empty strings and None return None."""
        assert normalize_customer_name(None) is None
        assert normalize_customer_name("") is None
        assert normalize_customer_name("   ") is None

    def test_complex_spanish_names(self):
        """Complex Spanish names with multiple accents."""
        assert normalize_customer_name("Construcción Ávila S.A.") == "CONSTRUCCION AVILA S.A."
        assert normalize_customer_name("Cerámicas del Perú") == "CERAMICAS DEL PERU"
        assert normalize_customer_name("Pisos Ñoño Ltda.") == "PISOS NONO LTDA."

    def test_unicode_normalization(self):
        """Different Unicode representations normalize the same."""
        # NFD (decomposed) vs NFC (composed) should both work
        name_composed = "café"  # NFC: e with acute as single char
        name_decomposed = "cafe\u0301"  # NFD: e + combining acute accent

        result_composed = normalize_customer_name(name_composed)
        result_decomposed = normalize_customer_name(name_decomposed)

        assert result_composed == "CAFE"
        assert result_decomposed == "CAFE"
        assert result_composed == result_decomposed


# ===================
# TEST 4: PRIORITY THRESHOLDS
# ===================

class TestPriorityThresholds:
    """
    Tests for _determine_priority and _determine_action_type methods.

    Priority is based on boat arrivals (via StockoutStatus).
    """

    def test_high_priority_stockout_before_next_boat(self, recommendation_service):
        """
        HIGH_PRIORITY when stockout expected before next boat arrives.
        StockoutStatus.HIGH_PRIORITY → RecommendationPriority.HIGH_PRIORITY
        """
        priority = recommendation_service._determine_priority(
            stockout_status=StockoutStatus.HIGH_PRIORITY,
        )

        assert priority == RecommendationPriority.HIGH_PRIORITY

    def test_consider_stockout_before_second_boat(self, recommendation_service):
        """
        CONSIDER when stockout expected before second boat arrives.
        StockoutStatus.CONSIDER → RecommendationPriority.CONSIDER
        """
        priority = recommendation_service._determine_priority(
            stockout_status=StockoutStatus.CONSIDER,
        )

        assert priority == RecommendationPriority.CONSIDER

    def test_well_covered_no_stockout_risk(self, recommendation_service):
        """
        WELL_COVERED when won't stock out for 2+ boat cycles.
        StockoutStatus.WELL_COVERED → RecommendationPriority.WELL_COVERED
        """
        priority = recommendation_service._determine_priority(
            stockout_status=StockoutStatus.WELL_COVERED,
        )

        assert priority == RecommendationPriority.WELL_COVERED

    def test_your_call_when_no_data(self, recommendation_service):
        """
        YOUR_CALL when no data / needs manual review.
        StockoutStatus.YOUR_CALL → RecommendationPriority.YOUR_CALL
        """
        priority = recommendation_service._determine_priority(
            stockout_status=StockoutStatus.YOUR_CALL,
        )

        assert priority == RecommendationPriority.YOUR_CALL

    def test_action_type_order_now(self, recommendation_service):
        """
        ORDER_NOW when HIGH_PRIORITY health status.
        """
        action = recommendation_service._determine_action_type(
            health_status=StockoutStatus.HIGH_PRIORITY,
            gap_pallets=Decimal("10"),
        )

        assert action == ActionType.ORDER_NOW

    def test_action_type_order_soon(self, recommendation_service):
        """
        ORDER_SOON when CONSIDER health status.
        """
        action = recommendation_service._determine_action_type(
            health_status=StockoutStatus.CONSIDER,
            gap_pallets=Decimal("10"),
        )

        assert action == ActionType.ORDER_SOON

    def test_action_type_well_stocked(self, recommendation_service):
        """
        WELL_STOCKED when WELL_COVERED and near target (gap > -5).
        """
        action = recommendation_service._determine_action_type(
            health_status=StockoutStatus.WELL_COVERED,
            gap_pallets=Decimal("-2"),  # 2 pallets over target (acceptable)
        )

        assert action == ActionType.WELL_STOCKED

    def test_action_type_skip_order(self, recommendation_service):
        """
        SKIP_ORDER when WELL_COVERED but significantly over target (gap < -5).
        """
        action = recommendation_service._determine_action_type(
            health_status=StockoutStatus.WELL_COVERED,
            gap_pallets=Decimal("-10"),  # 10 pallets over target
        )

        assert action == ActionType.SKIP_ORDER

    def test_action_type_review(self, recommendation_service):
        """
        REVIEW when YOUR_CALL / no data to make decision.
        """
        action = recommendation_service._determine_action_type(
            health_status=StockoutStatus.YOUR_CALL,
            gap_pallets=Decimal("5"),
        )

        assert action == ActionType.REVIEW

    def test_over_stock_threshold_boundary(self, recommendation_service):
        """
        Test the -5 pallet threshold for SKIP_ORDER vs WELL_STOCKED.
        """
        # Exactly at threshold (-5): should be WELL_STOCKED
        action_at_threshold = recommendation_service._determine_action_type(
            health_status=StockoutStatus.WELL_COVERED,
            gap_pallets=Decimal("-5"),
        )
        assert action_at_threshold == ActionType.WELL_STOCKED

        # Just over threshold (-5.01): should be SKIP_ORDER
        action_over_threshold = recommendation_service._determine_action_type(
            health_status=StockoutStatus.WELL_COVERED,
            gap_pallets=Decimal("-5.01"),
        )
        assert action_over_threshold == ActionType.SKIP_ORDER


# ===================
# INTEGRATION-STYLE TESTS
# ===================

class TestCalculationsIntegration:
    """
    Tests that verify multiple calculations work together correctly.
    """

    def test_coverage_gap_to_pallet_conversion(self, recommendation_service):
        """
        Verify coverage gap correctly converts m² to pallets.
        Uses M2_PER_PALLET = 135.
        """
        # 135 m² exactly = 1 pallet
        _, gap_m2, gap_pallets = recommendation_service._calculate_coverage_gap(
            daily_velocity=Decimal("135"),
            available_m2=Decimal("0"),
            days_to_cover=1,
        )

        assert gap_m2 == Decimal("135.00")
        assert gap_pallets == 1

        # 136 m² = 2 pallets (ceiling)
        _, gap_m2, gap_pallets = recommendation_service._calculate_coverage_gap(
            daily_velocity=Decimal("136"),
            available_m2=Decimal("0"),
            days_to_cover=1,
        )

        assert gap_m2 == Decimal("136.00")
        assert gap_pallets == 2  # ceil(136/135) = 2

    def test_confidence_affects_customer_metrics(self, recommendation_service):
        """
        Verify customer analysis data flows into metrics correctly.
        """
        weekly_sales = [Decimal("500")] * 8
        customer_analysis = {
            "unique_customers": 10,
            "top_customer_name": "Important Client",
            "top_customer_share": Decimal("0.15"),
            "recurring_count": 7,
            "recurring_share": Decimal("0.85"),
        }

        confidence, reason, cv, metrics = recommendation_service._calculate_confidence(
            weekly_sales=weekly_sales,
            weeks_of_data=8,
            customer_analysis=customer_analysis,
        )

        # Should be HIGH confidence with good customer diversity
        assert confidence == ConfidenceLevel.HIGH

        # Metrics should match input
        assert metrics["unique_customers"] == 10
        assert metrics["recurring_customers"] == 7
        assert metrics["top_customer_name"] == "Important Client"

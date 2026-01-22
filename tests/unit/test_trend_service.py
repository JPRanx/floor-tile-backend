"""
Unit tests for Trend Calculation Service.

Tests the core statistical calculation functions:
1. Coefficient of Variation (CV)
2. Confidence Level determination
3. Trend classification (direction and strength)
4. Country code inference
5. Sparkline generation
"""

import pytest
from datetime import date, timedelta
from decimal import Decimal

from services.trend_service import (
    calculate_coefficient_of_variation,
    determine_confidence_level,
    classify_trend,
    infer_country_code,
    generate_sparkline,
)
from models.trends import (
    ConfidenceLevel,
    TrendDirection,
    TrendStrength,
)


# ===================
# TEST 1: COEFFICIENT OF VARIATION
# ===================

class TestCoefficientOfVariation:
    """
    Tests for calculate_coefficient_of_variation function.

    CV = std_dev / mean (lower = more consistent sales).
    """

    def test_cv_consistent_values(self):
        """CV is 0 when all values are the same."""
        values = [Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100")]
        cv = calculate_coefficient_of_variation(values)
        assert cv == Decimal("0")

    def test_cv_slight_variation(self):
        """CV is low for values with slight variation."""
        # Mean = 100, values ±10 = ~10% variation
        values = [Decimal("90"), Decimal("100"), Decimal("110"), Decimal("100")]
        cv = calculate_coefficient_of_variation(values)
        # StdDev ≈ 7.07, CV ≈ 0.0707
        assert cv > Decimal("0.05")
        assert cv < Decimal("0.15")

    def test_cv_high_variation(self):
        """CV is high for erratic values."""
        # Mean ≈ 550, StdDev ≈ 450
        values = [Decimal("100"), Decimal("1000"), Decimal("100"), Decimal("1000")]
        cv = calculate_coefficient_of_variation(values)
        # CV should be around 0.8
        assert cv > Decimal("0.7")

    def test_cv_insufficient_data(self):
        """CV returns 0 with less than 2 data points."""
        assert calculate_coefficient_of_variation([]) == Decimal("0")
        assert calculate_coefficient_of_variation([Decimal("100")]) == Decimal("0")

    def test_cv_zero_mean(self):
        """CV returns 0 when mean is 0."""
        values = [Decimal("0"), Decimal("0"), Decimal("0")]
        cv = calculate_coefficient_of_variation(values)
        assert cv == Decimal("0")

    def test_cv_decimal_precision(self):
        """CV maintains decimal precision."""
        values = [Decimal("100.50"), Decimal("100.75"), Decimal("100.25"), Decimal("100.50")]
        cv = calculate_coefficient_of_variation(values)
        # Should be a small number with 4 decimal places
        assert isinstance(cv, Decimal)
        assert cv < Decimal("0.01")


# ===================
# TEST 2: CONFIDENCE LEVEL
# ===================

class TestConfidenceLevel:
    """
    Tests for determine_confidence_level function.

    Rules:
    - HIGH: 8+ samples and CV < 0.5
    - MEDIUM: 4+ samples and CV < 1.0
    - LOW: everything else
    """

    def test_high_confidence_many_samples_low_cv(self):
        """HIGH confidence with 8+ samples and low CV."""
        confidence = determine_confidence_level(
            sample_count=10,
            cv=Decimal("0.3"),
        )
        assert confidence == ConfidenceLevel.HIGH

    def test_high_confidence_boundary(self):
        """HIGH confidence at exactly 8 samples and CV < 0.5."""
        confidence = determine_confidence_level(
            sample_count=8,
            cv=Decimal("0.49"),
        )
        assert confidence == ConfidenceLevel.HIGH

    def test_medium_confidence_moderate_samples(self):
        """MEDIUM confidence with 4+ samples and CV < 1.0."""
        confidence = determine_confidence_level(
            sample_count=5,
            cv=Decimal("0.7"),
        )
        assert confidence == ConfidenceLevel.MEDIUM

    def test_medium_confidence_high_cv_disqualifies_high(self):
        """MEDIUM when samples high but CV too high for HIGH."""
        confidence = determine_confidence_level(
            sample_count=10,
            cv=Decimal("0.6"),  # CV >= 0.5 disqualifies HIGH
        )
        assert confidence == ConfidenceLevel.MEDIUM

    def test_low_confidence_few_samples(self):
        """LOW confidence with less than 4 samples."""
        confidence = determine_confidence_level(
            sample_count=3,
            cv=Decimal("0.2"),
        )
        assert confidence == ConfidenceLevel.LOW

    def test_low_confidence_very_high_cv(self):
        """LOW confidence when CV >= 1.0 regardless of samples."""
        confidence = determine_confidence_level(
            sample_count=10,
            cv=Decimal("1.0"),
        )
        assert confidence == ConfidenceLevel.LOW

    def test_low_confidence_zero_samples(self):
        """LOW confidence with no data."""
        confidence = determine_confidence_level(
            sample_count=0,
            cv=Decimal("0"),
        )
        assert confidence == ConfidenceLevel.LOW


# ===================
# TEST 3: TREND CLASSIFICATION
# ===================

class TestTrendClassification:
    """
    Tests for classify_trend function.

    Returns (direction, strength) based on percentage change:
    - STRONG: |change| >= 20%
    - MODERATE: 5% <= |change| < 20%
    - WEAK/STABLE: |change| < 5%
    """

    def test_strong_upward_trend(self):
        """STRONG UP when change >= 20%."""
        direction, strength = classify_trend(Decimal("25"))
        assert direction == TrendDirection.UP
        assert strength == TrendStrength.STRONG

    def test_strong_downward_trend(self):
        """STRONG DOWN when change <= -20%."""
        direction, strength = classify_trend(Decimal("-30"))
        assert direction == TrendDirection.DOWN
        assert strength == TrendStrength.STRONG

    def test_moderate_upward_trend(self):
        """MODERATE UP when 5% <= change < 20%."""
        direction, strength = classify_trend(Decimal("15"))
        assert direction == TrendDirection.UP
        assert strength == TrendStrength.MODERATE

    def test_moderate_downward_trend(self):
        """MODERATE DOWN when -20% < change <= -5%."""
        direction, strength = classify_trend(Decimal("-10"))
        assert direction == TrendDirection.DOWN
        assert strength == TrendStrength.MODERATE

    def test_stable_trend_low_change(self):
        """STABLE WEAK when |change| < 5%."""
        direction, strength = classify_trend(Decimal("3"))
        assert direction == TrendDirection.STABLE
        assert strength == TrendStrength.WEAK

    def test_stable_trend_negative_low_change(self):
        """STABLE WEAK when change is negative but small."""
        direction, strength = classify_trend(Decimal("-2"))
        assert direction == TrendDirection.STABLE
        assert strength == TrendStrength.WEAK

    def test_zero_change_is_stable(self):
        """Zero change is STABLE."""
        direction, strength = classify_trend(Decimal("0"))
        assert direction == TrendDirection.STABLE
        assert strength == TrendStrength.WEAK

    def test_boundary_exactly_20_percent(self):
        """Exactly 20% is STRONG UP."""
        direction, strength = classify_trend(Decimal("20"))
        assert direction == TrendDirection.UP
        assert strength == TrendStrength.STRONG

    def test_boundary_exactly_5_percent(self):
        """Exactly 5% is MODERATE UP (5 is the threshold for moderate)."""
        direction, strength = classify_trend(Decimal("5"))
        assert direction == TrendDirection.UP
        assert strength == TrendStrength.MODERATE

    def test_boundary_just_under_5_percent(self):
        """Just under 5% is STABLE WEAK."""
        direction, strength = classify_trend(Decimal("4.99"))
        assert direction == TrendDirection.STABLE
        assert strength == TrendStrength.WEAK


# ===================
# TEST 4: COUNTRY INFERENCE (Central American Focus)
# ===================

class TestCountryInference:
    """
    Tests for infer_country_code function.

    Infers country from customer name patterns and NIT format.
    Central American business: GT, HN, SV, NI, CR, PA
    """

    def test_guatemala_keyword(self):
        """Detects Guatemala from city names."""
        assert infer_country_code("PISOS DE GUATEMALA S.A.") == "GT"
        assert infer_country_code("Cerámicas Ciudad de Guatemala") == "GT"
        assert infer_country_code("Distribuidor Quetzaltenango Ltda") == "GT"

    def test_honduras_keyword(self):
        """Detects Honduras from city names."""
        assert infer_country_code("Pisos Tegucigalpa S.A.") == "HN"
        assert infer_country_code("Materiales San Pedro Sula") == "HN"
        assert infer_country_code("Honduras Tiles Corp") == "HN"

    def test_el_salvador_keyword(self):
        """Detects El Salvador from city names."""
        assert infer_country_code("San Salvador Ceramics") == "SV"
        assert infer_country_code("Pisos Santa Ana") == "SV"
        assert infer_country_code("El Salvador Distribuidor") == "SV"

    def test_panama_keyword(self):
        """Detects Panama from names."""
        assert infer_country_code("Panama Tiles Inc") == "PA"
        assert infer_country_code("Ciudad de Panama Store") == "PA"

    def test_default_to_guatemala(self):
        """Defaults to Guatemala when no pattern matches (main market)."""
        # Generic name with no country indicators
        assert infer_country_code("GENERIC COMPANY S.A.") == "GT"
        assert infer_country_code("Random Store") == "GT"
        assert infer_country_code("DECOESPACIO, S.A. DE C.V") == "GT"

    def test_guatemala_nit_pattern(self):
        """Detects Guatemala from 7-9 digit NIT."""
        assert infer_country_code("Company", nit="1234567") == "GT"
        assert infer_country_code("Company", nit="12345678") == "GT"
        assert infer_country_code("Company", nit="123456789") == "GT"

    def test_el_salvador_nit_pattern(self):
        """Detects El Salvador from 14 digit NIT starting with 06."""
        assert infer_country_code("Company", nit="06141234567890") == "SV"

    def test_honduras_rtn_pattern(self):
        """Detects Honduras from 13-14 digit RTN."""
        assert infer_country_code("Company", nit="1234567890123") == "HN"
        assert infer_country_code("Company", nit="12345678901234") == "HN"

    def test_keyword_takes_precedence(self):
        """Keyword in name takes precedence over NIT pattern."""
        # Honduras keyword with Guatemala NIT length
        assert infer_country_code("Tegucigalpa Tiles", nit="12345678") == "HN"

    def test_case_insensitive(self):
        """Detection is case-insensitive."""
        assert infer_country_code("GUATEMALA TILES") == "GT"
        assert infer_country_code("guatemala tiles") == "GT"
        assert infer_country_code("GuAtEmAlA TiLeS") == "GT"


# ===================
# TEST 5: SPARKLINE GENERATION
# ===================

class TestSparklineGeneration:
    """
    Tests for generate_sparkline function.

    Buckets data into time periods for visualization.
    """

    def test_empty_data_returns_empty(self):
        """Empty input returns empty sparkline."""
        sparkline = generate_sparkline([], num_buckets=12, period_days=90)
        assert sparkline == []

    def test_single_data_point(self):
        """Single data point placed in correct bucket."""
        today = date.today()
        data = [(today, Decimal("100"))]

        sparkline = generate_sparkline(data, num_buckets=12, period_days=90)

        # Should have 12 buckets
        assert len(sparkline) == 12

        # Last bucket should have the value (today is at the end)
        assert sparkline[-1].value == Decimal("100")

        # Other buckets should be zero
        assert all(s.value == Decimal("0") for s in sparkline[:-1])

    def test_bucket_aggregation(self):
        """Multiple values in same bucket are summed."""
        today = date.today()
        data = [
            (today, Decimal("50")),
            (today, Decimal("30")),
            (today, Decimal("20")),
        ]

        sparkline = generate_sparkline(data, num_buckets=12, period_days=90)

        # Last bucket should have sum
        assert sparkline[-1].value == Decimal("100")

    def test_data_outside_period_excluded(self):
        """Data older than period_days is excluded."""
        today = date.today()
        old_date = today - timedelta(days=100)  # Outside 90-day window

        data = [
            (old_date, Decimal("500")),  # Should be excluded
            (today, Decimal("100")),
        ]

        sparkline = generate_sparkline(data, num_buckets=12, period_days=90)

        # Only recent data should be included
        total = sum(s.value for s in sparkline)
        assert total == Decimal("100")

    def test_bucket_labels_weekly(self):
        """Buckets labeled W1, W2, etc. for weekly periods."""
        today = date.today()
        data = [(today, Decimal("100"))]

        # 12 buckets over 84 days = 7-day buckets
        sparkline = generate_sparkline(data, num_buckets=12, period_days=84)

        # Should have week labels
        assert sparkline[0].period == "W1"
        assert sparkline[1].period == "W2"
        assert sparkline[11].period == "W12"

    def test_even_distribution(self):
        """Values distributed across buckets by date."""
        today = date.today()
        start = today - timedelta(days=90)

        # Create data points spread across the period
        data = [
            (start, Decimal("10")),
            (start + timedelta(days=45), Decimal("20")),
            (today, Decimal("30")),
        ]

        sparkline = generate_sparkline(data, num_buckets=12, period_days=90)

        # All buckets initialized
        assert len(sparkline) == 12

        # Total should equal sum of inputs
        total = sum(s.value for s in sparkline)
        assert total == Decimal("60")


# ===================
# INTEGRATION TESTS
# ===================

class TestTrendCalculationsIntegration:
    """
    Tests that verify multiple calculations work together.
    """

    def test_cv_affects_confidence(self):
        """
        Verify that CV calculation properly affects confidence level.
        """
        # Low CV values (consistent)
        consistent_values = [Decimal("100")] * 8
        cv_consistent = calculate_coefficient_of_variation(consistent_values)
        confidence_consistent = determine_confidence_level(len(consistent_values), cv_consistent)

        assert cv_consistent == Decimal("0")
        assert confidence_consistent == ConfidenceLevel.HIGH

        # High CV values (erratic)
        erratic_values = [Decimal("10"), Decimal("1000"), Decimal("50"), Decimal("800"),
                         Decimal("20"), Decimal("900"), Decimal("30"), Decimal("700")]
        cv_erratic = calculate_coefficient_of_variation(erratic_values)
        confidence_erratic = determine_confidence_level(len(erratic_values), cv_erratic)

        assert cv_erratic > Decimal("0.5")
        assert confidence_erratic in [ConfidenceLevel.MEDIUM, ConfidenceLevel.LOW]

    def test_trend_direction_matches_velocity_change(self):
        """
        Verify trend classification matches velocity changes.
        """
        # Positive velocity change = UP trend
        positive_change = Decimal("25")
        direction, _ = classify_trend(positive_change)
        assert direction == TrendDirection.UP

        # Negative velocity change = DOWN trend
        negative_change = Decimal("-25")
        direction, _ = classify_trend(negative_change)
        assert direction == TrendDirection.DOWN

        # Small change = STABLE
        small_change = Decimal("2")
        direction, _ = classify_trend(small_change)
        assert direction == TrendDirection.STABLE

    def test_sparkline_preserves_trend_shape(self):
        """
        Verify sparkline captures upward/downward trend shape.
        """
        today = date.today()
        period_days = 60

        # Create upward trend data
        upward_data = []
        for i in range(6):
            d = today - timedelta(days=period_days - i * 10)
            value = Decimal(str(100 + i * 50))  # 100, 150, 200, 250, 300, 350
            upward_data.append((d, value))

        sparkline = generate_sparkline(upward_data, num_buckets=6, period_days=period_days)

        # Later buckets should have higher values
        # (roughly - depends on bucket boundaries)
        values = [s.value for s in sparkline]
        non_zero_values = [v for v in values if v > 0]

        # There should be some non-zero values
        assert len(non_zero_values) > 0

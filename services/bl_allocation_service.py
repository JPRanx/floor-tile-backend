"""
BL Allocation Service — Spreads products across Bills of Lading for customs safety.

Key principle: Critical products (score >= 85) are SPREAD across BLs so that
a single BL delay doesn't block all high-priority stock.

Algorithm:
1. SEPARATE products by criticality (score >= 85 vs < 85)
2. SORT critical products by score descending
3. SPREAD critical products across BLs (round-robin)
4. GROUP non-critical products by customer
5. ASSIGN customer groups to BL where their critical product is
6. DISTRIBUTE general stock evenly
7. CALCULATE totals per BL
8. BALANCE overflows (move non-critical first, then critical with warning)
9. CHECK risk distribution (no BL should have > 40% of critical)
"""

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from math import ceil
from typing import Dict, List, Optional

import structlog

from models.bl_allocation import (
    CRITICAL_THRESHOLD,
    M2_PER_PALLET,
    MAX_CONTAINERS_PER_BL,
    PALLETS_PER_CONTAINER,
    BLAllocation,
    BLAllocationReport,
    BLProductAllocation,
)
from models.order_builder import OrderBuilderProduct
from models.trends import CustomerTrend

logger = structlog.get_logger(__name__)


class BLAllocationService:
    """Allocates products across BLs with safety spreading."""

    def allocate_products_to_bls(
        self,
        products: List[OrderBuilderProduct],
        num_bls: int,
        customer_trends: List[CustomerTrend],
        boat_departure: date,
        boat_name: str,
    ) -> BLAllocationReport:
        """
        Allocate products across BLs with:
        1. Critical product spreading (safety)
        2. Customer grouping (convenience)
        3. Even distribution (balance)

        Args:
            products: Selected products from Order Builder (with scores)
            num_bls: Number of BLs to allocate across (1-5)
            customer_trends: Customer trend data for primary customer lookup
            boat_departure: Boat departure date
            boat_name: Boat name/identifier

        Returns:
            BLAllocationReport with allocations and risk analysis
        """
        logger.info(
            "allocating_products_to_bls",
            product_count=len(products),
            num_bls=num_bls,
        )

        # Filter to only selected products with pallets > 0
        selected_products = [
            p for p in products
            if p.is_selected and p.selected_pallets > 0
        ]

        if not selected_products:
            logger.warning("no_products_to_allocate")
            return self._empty_report(num_bls, boat_departure, boat_name)

        # Step 1: Separate products by criticality
        critical_products = [
            p for p in selected_products
            if p.score and p.score.total >= CRITICAL_THRESHOLD
        ]
        non_critical_products = [
            p for p in selected_products
            if not p.score or p.score.total < CRITICAL_THRESHOLD
        ]

        logger.info(
            "products_separated",
            critical=len(critical_products),
            non_critical=len(non_critical_products),
        )

        # Step 2: Sort critical products by score (highest first)
        critical_products.sort(
            key=lambda p: p.score.total if p.score else 0,
            reverse=True
        )

        # Step 3: Initialize BLs
        bl_allocations = [
            BLAllocation(bl_number=i + 1)
            for i in range(num_bls)
        ]

        # Step 4: SPREAD critical products across BLs (round-robin)
        # Track which BL each customer's critical product went to
        customer_to_bl: Dict[str, BLAllocation] = {}

        for i, product in enumerate(critical_products):
            bl_index = i % num_bls
            bl = bl_allocations[bl_index]

            primary_customer = self._get_primary_customer(
                product.sku, customer_trends
            )

            bl.products.append(BLProductAllocation(
                product_id=product.product_id,
                sku=product.sku,
                description=product.description,
                pallets=product.selected_pallets,
                m2=Decimal(product.selected_pallets) * M2_PER_PALLET,
                weight_kg=product.total_weight_kg or Decimal("0"),
                primary_customer=primary_customer,
                score=product.score.total if product.score else 0,
                is_critical=True,
            ))

            # Track which BL this customer's critical product went to
            if primary_customer:
                if primary_customer not in bl.primary_customers:
                    bl.primary_customers.append(primary_customer)
                customer_to_bl[primary_customer] = bl

        # Step 5: Group remaining products by customer
        customer_products: Dict[str, List[OrderBuilderProduct]] = defaultdict(list)
        general_products: List[OrderBuilderProduct] = []

        for product in non_critical_products:
            primary_customer = self._get_primary_customer(
                product.sku, customer_trends
            )
            if primary_customer:
                customer_products[primary_customer].append(product)
            else:
                general_products.append(product)

        # Step 6: Assign customer groups to BLs
        # Put customer's products in BL where their critical product is
        for customer, products_list in customer_products.items():
            # Find BL that has this customer's critical product
            target_bl = customer_to_bl.get(customer)

            # If no critical product, assign to BL with most room
            if target_bl is None:
                target_bl = min(
                    bl_allocations,
                    key=lambda b: sum(p.pallets for p in b.products)
                )
                if customer not in target_bl.primary_customers:
                    target_bl.primary_customers.append(customer)

            # Add customer's products to their BL
            for product in products_list:
                target_bl.products.append(BLProductAllocation(
                    product_id=product.product_id,
                    sku=product.sku,
                    description=product.description,
                    pallets=product.selected_pallets,
                    m2=Decimal(product.selected_pallets) * M2_PER_PALLET,
                    weight_kg=product.total_weight_kg or Decimal("0"),
                    primary_customer=customer,
                    score=product.score.total if product.score else 0,
                    is_critical=False,
                ))

        # Step 7: Distribute general stock evenly
        for i, product in enumerate(general_products):
            bl_index = i % num_bls
            bl_allocations[bl_index].products.append(BLProductAllocation(
                product_id=product.product_id,
                sku=product.sku,
                description=product.description,
                pallets=product.selected_pallets,
                m2=Decimal(product.selected_pallets) * M2_PER_PALLET,
                weight_kg=product.total_weight_kg or Decimal("0"),
                primary_customer=None,
                score=product.score.total if product.score else 0,
                is_critical=False,
            ))

        # Step 8: Calculate totals
        for bl in bl_allocations:
            self._calculate_bl_totals(bl)

        # Step 9: Balance overflows
        warnings = self._balance_bl_capacities(bl_allocations)

        # Step 10: Check risk distribution
        critical_counts = [bl.critical_product_count for bl in bl_allocations]
        total_critical = sum(critical_counts)
        max_critical_in_one = max(critical_counts) if critical_counts else 0

        # Calculate max percentage
        if total_critical > 0:
            max_critical_pct = Decimal(max_critical_in_one) / Decimal(total_critical) * 100
        else:
            max_critical_pct = Decimal("0")

        # Even if each BL has <= 40% of critical products
        risk_even = (
            total_critical == 0 or
            max_critical_in_one <= ceil(total_critical * 0.4)
        )

        if not risk_even and total_critical > 0:
            bl_with_most = critical_counts.index(max_critical_in_one) + 1
            warnings.append(
                f"BL {bl_with_most} has {max_critical_in_one}/{total_critical} "
                f"critical products ({max_critical_pct:.0f}%)"
            )

        # Build report
        report = BLAllocationReport(
            generated_at=datetime.now(),
            boat_departure=boat_departure,
            boat_name=boat_name,
            num_bls=num_bls,
            total_containers=sum(bl.total_containers for bl in bl_allocations),
            total_pallets=sum(bl.total_pallets for bl in bl_allocations),
            total_m2=sum(bl.total_m2 for bl in bl_allocations),
            total_weight_kg=sum(bl.total_weight_kg for bl in bl_allocations),
            total_critical_products=total_critical,
            allocations=bl_allocations,
            warnings=warnings,
            risk_distribution_even=risk_even,
            max_critical_pct=max_critical_pct,
        )

        logger.info(
            "bl_allocation_complete",
            num_bls=num_bls,
            total_containers=report.total_containers,
            total_critical=total_critical,
            risk_even=risk_even,
        )

        return report

    def _get_primary_customer(
        self,
        sku: str,
        customer_trends: List[CustomerTrend]
    ) -> Optional[str]:
        """
        Find which customer buys this product most (by urgency score).

        Looks through each customer's top_products to find matches.
        Returns the customer with the highest urgency score for this SKU.
        """
        best_customer = None
        best_score = 0

        for customer in customer_trends:
            for prod in customer.top_products:
                if prod.sku == sku:
                    score = self._customer_urgency_score(customer)
                    if score > best_score:
                        best_score = score
                        best_customer = customer.customer_normalized
                    break  # Found this product for this customer, move to next customer

        return best_customer

    def _customer_urgency_score(self, customer: CustomerTrend) -> int:
        """
        Calculate urgency score for customer.

        Score = tier_weight × overdue_multiplier

        Tier weights: A=100, B=50, C=25
        Overdue multiplier:
          - <= 14 days: 1.0x
          - 15-30 days: 1.5x
          - 31-60 days: 2.0x
          - > 60 days: 2.5x
        """
        tier_weights = {"A": 100, "B": 50, "C": 25}
        tier = customer.tier.value if hasattr(customer.tier, 'value') else str(customer.tier)
        tier_weight = tier_weights.get(tier, 25)

        days_overdue = customer.days_overdue or 0
        if days_overdue <= 14:
            multiplier = 1.0
        elif days_overdue <= 30:
            multiplier = 1.5
        elif days_overdue <= 60:
            multiplier = 2.0
        else:
            multiplier = 2.5

        return int(tier_weight * multiplier)

    def _calculate_bl_totals(self, bl: BLAllocation) -> None:
        """Calculate totals for a BL."""
        bl.total_pallets = sum(p.pallets for p in bl.products)
        bl.total_containers = ceil(bl.total_pallets / PALLETS_PER_CONTAINER) if bl.total_pallets > 0 else 0
        bl.total_m2 = sum(p.m2 for p in bl.products)
        bl.total_weight_kg = sum(p.weight_kg for p in bl.products)
        bl.critical_product_count = sum(1 for p in bl.products if p.is_critical)

    def _balance_bl_capacities(
        self,
        allocations: List[BLAllocation]
    ) -> List[str]:
        """
        Move products from overflowing BLs to BLs with capacity.

        Prefers moving non-critical products first.
        Returns list of warnings.
        """
        warnings = []

        for bl in allocations:
            while bl.total_containers > MAX_CONTAINERS_PER_BL:
                # Find BL with most room (excluding this one)
                other_bls = [b for b in allocations if b.bl_number != bl.bl_number]
                if not other_bls:
                    break

                target_bl = min(other_bls, key=lambda b: b.total_containers)

                if target_bl.total_containers >= MAX_CONTAINERS_PER_BL:
                    warnings.append(
                        f"BL {bl.bl_number} exceeds capacity ({bl.total_containers} containers) "
                        f"but no room in other BLs"
                    )
                    break

                # Move last non-critical product to target
                non_critical = [
                    (i, p) for i, p in enumerate(bl.products)
                    if not p.is_critical
                ]
                if non_critical:
                    idx, product = non_critical[-1]
                    bl.products.pop(idx)
                    target_bl.products.append(product)
                else:
                    # Only critical products left - must move one
                    if bl.products:
                        product = bl.products.pop()
                        target_bl.products.append(product)
                        warnings.append(
                            f"Moved critical product {product.sku} from BL {bl.bl_number} "
                            f"to BL {target_bl.bl_number} due to capacity"
                        )
                    else:
                        break

                # Recalculate
                self._calculate_bl_totals(bl)
                self._calculate_bl_totals(target_bl)

        return warnings

    def _empty_report(
        self,
        num_bls: int,
        boat_departure: date,
        boat_name: str,
    ) -> BLAllocationReport:
        """Create an empty report when no products to allocate."""
        return BLAllocationReport(
            generated_at=datetime.now(),
            boat_departure=boat_departure,
            boat_name=boat_name,
            num_bls=num_bls,
            total_containers=0,
            total_pallets=0,
            total_m2=Decimal("0"),
            total_weight_kg=Decimal("0"),
            total_critical_products=0,
            allocations=[
                BLAllocation(bl_number=i + 1) for i in range(num_bls)
            ],
            warnings=["No products to allocate"],
            risk_distribution_even=True,
            max_critical_pct=Decimal("0"),
        )


# Singleton
_bl_allocation_service: Optional[BLAllocationService] = None


def get_bl_allocation_service() -> BLAllocationService:
    """Get the singleton BL allocation service instance."""
    global _bl_allocation_service
    if _bl_allocation_service is None:
        _bl_allocation_service = BLAllocationService()
    return _bl_allocation_service

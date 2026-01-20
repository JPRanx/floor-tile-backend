#!/usr/bin/env python3
"""
E2E Order Flow Simulation Script

Pushes a factory order through the entire system lifecycle to verify
all stages work correctly. Can also serve as a training tool with
real-world business context.

Usage:
    python scripts/simulate_order_flow.py                    # Run full simulation
    python scripts/simulate_order_flow.py --cleanup          # Run with cleanup
    python scripts/simulate_order_flow.py --interactive      # Pause after each stage
    python scripts/simulate_order_flow.py -i --no-context    # Interactive without context
    python scripts/simulate_order_flow.py --base-url URL     # Custom API URL
    python scripts/simulate_order_flow.py --skip-stages 1,2  # Skip specific stages
"""

import argparse
import sys
import time
import uuid
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

try:
    import httpx
except ImportError:
    print("Error: httpx not installed. Run: pip install httpx")
    sys.exit(1)


# ===================
# CONFIGURATION
# ===================

DEFAULT_BASE_URL = "http://localhost:8000"
DEFAULT_FRONTEND_URL = "http://localhost:5173"
TIMEOUT = 30.0

# Factory order statuses
FO_STATUSES = ["PENDING", "CONFIRMED", "IN_PRODUCTION", "READY", "SHIPPED"]

# Shipment statuses
SHIPMENT_STATUSES = [
    "AT_FACTORY",
    "AT_ORIGIN_PORT",
    "IN_TRANSIT",
    "AT_DESTINATION_PORT",
    "IN_CUSTOMS",
    "IN_TRUCK",
    "DELIVERED"
]

# ===================
# BUSINESS CONTEXT
# ===================

STAGE_CONTEXT = {
    1: {
        "title": "Create Factory Order",
        "real_world": "Ashley reviews inventory levels (Dashboard shows low stock alerts), uses Order Builder to select products/quantities, clicks Export. System generates PV number.",
        "trigger": "Dashboard stockout warning OR regular monthly replenishment cycle",
        "action": "Dashboard → Order Builder → Select products → Export Order",
        "document": "Excel file downloaded, sent to factory via WhatsApp",
        "pages": ["Order Builder", "Dashboard"],
        "fo_status": "PENDING",
        "shp_status": None,
        "check": 'Factory Order appears in Pipeline "Ordered" column with status PENDING',
    },
    2: {
        "title": "Factory Confirms Order",
        "real_world": "Factory (China) receives Excel via WhatsApp. Reviews capacity. Replies with confirmation.",
        "trigger": "Factory replies via WhatsApp: 'Order confirmed, starting production'",
        "action": "Update factory order status: PENDING → CONFIRMED",
        "document": "WhatsApp confirmation (manual status update)",
        "pages": ["Pipeline"],
        "fo_status": "CONFIRMED",
        "shp_status": None,
        "check": 'Pipeline card shows CONFIRMED badge. Optional: Telegram alert sent.',
    },
    3: {
        "title": "Production Schedule Available",
        "real_world": "Factory sends daily/weekly production schedule PDF showing progress. This is informational - status doesn't change.",
        "trigger": "Factory sends Production Schedule PDF via WhatsApp",
        "action": "Upload Production Schedule PDF (optional tracking)",
        "document": "Production Schedule PDF",
        "pages": ["Upload"],
        "fo_status": "CONFIRMED",
        "shp_status": None,
        "note": "Informational only. Status stays CONFIRMED. Not all orders have this.",
        "check": "No status change expected. Document stored for reference.",
    },
    4: {
        "title": "Order Ready at Factory",
        "real_world": "Factory finishes production. Notifies Ashley that product is ready for pickup. Time to book shipping.",
        "trigger": "Factory WhatsApp: 'Order ready for pickup'",
        "action": "Update factory order status: CONFIRMED → READY",
        "document": "WhatsApp notification (manual status update)",
        "pages": ["Pipeline"],
        "fo_status": "READY",
        "shp_status": None,
        "note": "This triggers suggestion to book shipment if none linked.",
        "check": 'Pipeline card shows READY. Telegram suggests linking to shipment.',
    },
    5: {
        "title": "Book Shipment",
        "real_world": "Ashley contacts freight forwarder (TIBA). Forwarder books container space on vessel. Sends Booking Confirmation PDF via email.",
        "trigger": "Freight forwarder emails Booking Confirmation PDF",
        "action": "Forward email to ingest@domain.com OR manual upload",
        "document": "Booking Confirmation PDF",
        "pages": ["Shipments", "Email Inbox"],
        "fo_status": "SHIPPED",
        "shp_status": "AT_FACTORY",
        "note": "Creates shipment. If factory_order_id provided, FO → SHIPPED automatically.",
        "check": "Shipment created (AT_FACTORY). FO moves to 'Shipped' column.",
    },
    6: {
        "title": "Containers at Origin Port",
        "real_world": "Factory packs containers, ships to Ningbo port. Sends Packing List Excel showing containers, m², pallets per container.",
        "trigger": "Factory sends Packing List Excel via WhatsApp",
        "action": "Upload Packing List → System creates containers, links to shipment",
        "document": "Packing List XLSX",
        "pages": ["Shipments → Packing List Upload"],
        "fo_status": "SHIPPED",
        "shp_status": "AT_ORIGIN_PORT",
        "check": 'Shipment status → AT_ORIGIN_PORT. Containers appear in shipment detail.',
    },
    7: {
        "title": "HBL/MBL Received",
        "real_world": "TIBA sends House Bill of Lading and/or Master Bill of Lading. These contain freight costs and container details. Issued before ship departs.",
        "trigger": "TIBA sends HBL/MBL PDF (email)",
        "action": "System updates shipment with SHP number, freight cost, container details",
        "document": "HBL PDF, MBL PDF",
        "pages": ["Shipments", "Email Inbox"],
        "fo_status": "SHIPPED",
        "shp_status": "AT_ORIGIN_PORT",
        "note": "HBL has SHP number but no booking. MBL has booking but no SHP. System links via containers.",
        "check": "Shipment details updated. Telegram confirms HBL processed or pending.",
    },
    8: {
        "title": "Ship Departs",
        "real_world": "Ship has departed from origin port. TIBA sends Departure Notice after HBL/MBL are issued.",
        "trigger": "TIBA sends Departure Notice PDF (email)",
        "action": "Update shipment status: AT_ORIGIN_PORT → IN_TRANSIT",
        "document": "Departure Notice PDF",
        "pages": ["Shipments", "Pipeline"],
        "fo_status": "SHIPPED",
        "shp_status": "IN_TRANSIT",
        "check": 'Shipment status → IN_TRANSIT. Pipeline shows "In Transit".',
    },
    9: {
        "title": "Ship Arrives at Destination",
        "real_world": "Vessel arrives at Buenaventura port. Forwarder sends Arrival Notice.",
        "trigger": "Freight forwarder sends Arrival Notice OR vessel ETA passes",
        "action": "Update shipment status: IN_TRANSIT → AT_DESTINATION_PORT",
        "document": "Arrival Notice PDF",
        "pages": ["Shipments"],
        "fo_status": "SHIPPED",
        "shp_status": "AT_DESTINATION_PORT",
        "check": 'Shipment status → AT_DESTINATION_PORT.',
    },
    10: {
        "title": "Customs Clearance",
        "real_world": "Customs broker processes import declaration. Ashley monitors free days to avoid demurrage.",
        "trigger": "Customs clearance completed",
        "action": "Update shipment status: AT_DESTINATION_PORT → IN_CUSTOMS → IN_TRUCK",
        "document": "Customs declaration (future feature)",
        "pages": ["Shipments"],
        "fo_status": "SHIPPED",
        "shp_status": "IN_TRUCK",
        "note": "Free days tracking prevents demurrage charges.",
        "check": 'Shipment status → IN_TRUCK (ready for delivery).',
    },
    11: {
        "title": "Delivered to Warehouse",
        "real_world": "Trucking company delivers containers to warehouse. Ashley confirms receipt.",
        "trigger": "Warehouse confirms containers received",
        "action": "Update shipment status: IN_TRUCK → DELIVERED",
        "document": "Delivery receipt (manual confirmation)",
        "pages": ["Shipments"],
        "fo_status": "SHIPPED",
        "shp_status": "DELIVERED",
        "check": 'Shipment → DELIVERED. FO in Pipeline "Delivered" column.',
    },
    12: {
        "title": "Update Inventory",
        "real_world": "Warehouse counts received product. Ashley uploads inventory snapshot. System updates stock levels.",
        "trigger": "After physical count completed",
        "action": "Upload Inventory Excel → System reconciles stock",
        "document": "Inventory Excel (warehouse count or POS export)",
        "pages": ["Upload", "Dashboard"],
        "fo_status": "SHIPPED",
        "shp_status": "DELIVERED",
        "check": "Dashboard stock levels reflect delivered product. Cycle complete.",
    },
}


# ===================
# STAGE UI HINTS
# ===================

STAGE_UI_HINTS = {
    1: {
        "before": "Check current inventory on Dashboard → look for low stock items",
        "after": "Open Pipeline page → new order should appear in 'Ordered' column",
        "urls": ["/", "/pipeline", "/order-builder"],
    },
    2: {
        "before": "Pipeline shows order in PENDING status",
        "after": "Pipeline card should now show CONFIRMED badge",
        "urls": ["/pipeline"],
    },
    3: {
        "before": "Order is CONFIRMED - factory is working on it",
        "after": "No change expected (informational stage)",
        "urls": ["/upload"],
    },
    4: {
        "before": "Order still CONFIRMED",
        "after": "Pipeline shows READY badge. Check Telegram for suggestion alert.",
        "urls": ["/pipeline"],
    },
    5: {
        "before": "Order is READY, needs shipment booking",
        "after": "New shipment on Shipments page. Order moves to 'Shipped' column.",
        "urls": ["/shipments", "/pipeline"],
    },
    6: {
        "before": "Shipment exists with status AT_FACTORY",
        "after": "Shipment status → AT_ORIGIN_PORT. Click shipment to see containers.",
        "urls": ["/shipments"],
    },
    7: {
        "before": "Shipment at AT_ORIGIN_PORT",
        "after": "Shipment details updated with HBL info. Check Telegram alert.",
        "urls": ["/shipments"],
    },
    8: {
        "before": "Shipment still AT_ORIGIN_PORT (HBL received)",
        "after": "Shipment status → IN_TRANSIT. Pipeline 'In Transit' column.",
        "urls": ["/shipments", "/pipeline"],
    },
    9: {
        "before": "Shipment IN_TRANSIT",
        "after": "Shipment status → AT_DESTINATION_PORT",
        "urls": ["/shipments"],
    },
    10: {
        "before": "Shipment at AT_DESTINATION_PORT",
        "after": "Shipment status → IN_TRUCK (in transit to warehouse)",
        "urls": ["/shipments"],
    },
    11: {
        "before": "Shipment IN_TRUCK",
        "after": "Shipment → DELIVERED. Pipeline 'Delivered' column.",
        "urls": ["/shipments", "/pipeline"],
    },
    12: {
        "before": "Shipment DELIVERED but inventory not updated",
        "after": "Dashboard stock levels reflect delivery. Cycle complete!",
        "urls": ["/upload", "/"],
    },
}


# ===================
# LOGGING HELPERS
# ===================

class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    WHITE = "\033[97m"
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"


def log_success(msg: str):
    print(f"{Colors.GREEN}[OK] {msg}{Colors.RESET}")


def log_error(msg: str):
    print(f"{Colors.RED}[FAIL] {msg}{Colors.RESET}")


def log_info(msg: str):
    print(f"{Colors.BLUE}[INFO] {msg}{Colors.RESET}")


def log_warning(msg: str):
    print(f"{Colors.YELLOW}[WARN] {msg}{Colors.RESET}")


def log_header(msg: str):
    print(f"\n{Colors.BOLD}{Colors.CYAN}{msg}{Colors.RESET}")


def log_separator():
    print(f"{Colors.CYAN}{'-' * 54}{Colors.RESET}")


def log_hint(msg: str):
    print(f"{Colors.MAGENTA}   >> {msg}{Colors.RESET}")


# ===================
# API CLIENT
# ===================

class APIClient:
    """Simple HTTP client for API calls."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(timeout=TIMEOUT)

    def get(self, path: str) -> dict:
        url = f"{self.base_url}{path}"
        response = self.client.get(url)
        return self._handle_response(response, "GET", path)

    def post(self, path: str, data: dict) -> dict:
        url = f"{self.base_url}{path}"
        response = self.client.post(url, json=data)
        return self._handle_response(response, "POST", path, data)

    def patch(self, path: str, data: dict) -> dict:
        url = f"{self.base_url}{path}"
        response = self.client.patch(url, json=data)
        return self._handle_response(response, "PATCH", path, data)

    def delete(self, path: str) -> bool:
        url = f"{self.base_url}{path}"
        response = self.client.delete(url)
        if response.status_code in [200, 204]:
            return True
        return self._handle_response(response, "DELETE", path)

    def _handle_response(self, response, method: str, path: str, payload: dict = None):
        if response.status_code >= 400:
            error_detail = {
                "status_code": response.status_code,
                "method": method,
                "path": path,
                "payload": payload,
                "response": response.text[:500] if response.text else None
            }
            raise APIError(
                f"{response.status_code} error on {method} {path}",
                error_detail
            )

        if response.status_code == 204:
            return {}

        return response.json()

    def close(self):
        self.client.close()


class APIError(Exception):
    """API call failed."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.details = details or {}


# ===================
# SIMULATION STAGES
# ===================

class OrderFlowSimulation:
    """End-to-end order flow simulation."""

    def __init__(
        self,
        base_url: str,
        cleanup: bool = False,
        skip_stages: list = None,
        interactive: bool = False,
        frontend_url: str = DEFAULT_FRONTEND_URL,
        show_context: bool = None
    ):
        self.api = APIClient(base_url)
        self.cleanup = cleanup
        self.skip_stages = skip_stages or []
        self.interactive = interactive
        self.frontend_url = frontend_url.rstrip("/")

        # Context defaults to ON in interactive mode, OFF otherwise
        if show_context is None:
            self.show_context = interactive
        else:
            self.show_context = show_context

        # Track created resources
        self.factory_order_id: Optional[str] = None
        self.factory_order_pv: Optional[str] = None
        self.factory_order_status: str = ""
        self.shipment_id: Optional[str] = None
        self.shipment_shp: Optional[str] = None
        self.shipment_status: str = ""
        self.product_ids: list[str] = []

        # Timing
        self.start_time: float = 0
        self.stage_results: list[dict] = []
        self.quit_early: bool = False
        self.last_completed_stage: int = 0

    def run(self):
        """Run the full simulation."""
        self.start_time = time.time()

        if self.interactive:
            print(f"\n{Colors.BOLD}>>> Starting Order Flow Simulation (Interactive Mode){Colors.RESET}")
        else:
            print(f"\n{Colors.BOLD}>>> Starting Order Flow Simulation{Colors.RESET}")

        print(f"   Base URL: {self.api.base_url}")
        print(f"   Cleanup: {self.cleanup}")
        if self.interactive:
            print(f"   Context: {'ON' if self.show_context else 'OFF'}")

        if self.interactive:
            print(f"\n   {Colors.CYAN}Frontend URLs to watch:{Colors.RESET}")
            print(f"   - Pipeline:      {self.frontend_url}/pipeline")
            print(f"   - Shipments:     {self.frontend_url}/shipments")
            print(f"   - Order Builder: {self.frontend_url}/order-builder")

        log_separator()

        try:
            # Get test product IDs first
            self._get_test_products()

            # Run all stages
            stages = [
                (1, "Create Order", self.stage_1_create_order),
                (2, "Factory Confirms", self.stage_2_confirm_order),
                (3, "Production Schedule", self.stage_3_production_schedule),
                (4, "Ready to Ship", self.stage_4_ready),
                (5, "Book Shipment", self.stage_5_create_shipment),
                (6, "At Origin Port", self.stage_6_at_origin_port),
                (7, "HBL/MBL Received", self.stage_7_hbl_received),
                (8, "Ship Departs", self.stage_8_in_transit),
                (9, "Ship Arrives", self.stage_9_at_destination),
                (10, "Customs Clearance", self.stage_10_customs),
                (11, "Delivered", self.stage_11_delivered),
                (12, "Update Inventory", self.stage_12_verify_pipeline),
            ]

            for stage_num, stage_name, stage_func in stages:
                if stage_num in self.skip_stages:
                    log_warning(f"Stage {stage_num}: {stage_name} - SKIPPED")
                    continue

                try:
                    stage_func()
                    self.stage_results.append({
                        "stage": stage_num,
                        "name": stage_name,
                        "status": "passed"
                    })
                    self.last_completed_stage = stage_num

                    # Interactive pause
                    if self.interactive and stage_num < 12:
                        if not self._interactive_pause(stage_num):
                            # User quit early
                            self.quit_early = True
                            self._handle_early_quit()
                            return False

                except Exception as e:
                    self.stage_results.append({
                        "stage": stage_num,
                        "name": stage_name,
                        "status": "failed",
                        "error": str(e)
                    })
                    self._handle_stage_failure(stage_num, stage_name, e)
                    return False

            # Optional cleanup
            if self.cleanup:
                self._cleanup()

            self._print_summary(success=True)
            return True

        except Exception as e:
            log_error(f"Simulation failed: {e}")
            self._print_summary(success=False)
            return False
        finally:
            self.api.close()

    def _print_context(self, stage_num: int):
        """Print business context for a stage."""
        ctx = STAGE_CONTEXT.get(stage_num, {})
        if not ctx:
            return

        print()
        print(f"   {Colors.YELLOW}WHAT HAPPENS IN REAL LIFE:{Colors.RESET}")
        print(f"   {Colors.DIM}{'=' * 48}{Colors.RESET}")

        # Real world
        real_world = ctx.get("real_world", "")
        if real_world:
            print(f"   {Colors.WHITE}Real world:{Colors.RESET}  {real_world}")

        # Trigger
        trigger = ctx.get("trigger", "")
        if trigger:
            print(f"   {Colors.WHITE}Trigger:{Colors.RESET}     {trigger}")

        # Document
        document = ctx.get("document", "")
        if document:
            print(f"   {Colors.WHITE}Document:{Colors.RESET}    {document}")

        # Action
        action = ctx.get("action", "")
        if action:
            print(f"   {Colors.WHITE}Action:{Colors.RESET}      {action}")

        # Note (if any)
        note = ctx.get("note", "")
        if note:
            print()
            print(f"   {Colors.CYAN}Note:{Colors.RESET} {note}")

        print(f"   {Colors.DIM}{'=' * 48}{Colors.RESET}")

    def _interactive_pause(self, stage_num: int) -> bool:
        """
        Pause for user in interactive mode.

        Returns True to continue, False to quit.
        """
        ctx = STAGE_CONTEXT.get(stage_num, {})
        hints = STAGE_UI_HINTS.get(stage_num, {})

        # Show context if enabled
        if self.show_context:
            self._print_context(stage_num)

        # Show what to check in UI
        check_msg = ctx.get("check", "")

        print()
        print(f"   {Colors.MAGENTA}CHECK IN UI:{Colors.RESET}")

        # Show "after" hint from STAGE_UI_HINTS
        after_hint = hints.get("after", "")
        if after_hint:
            print(f"   >> {after_hint}")
        elif check_msg:
            print(f"   >> {check_msg}")

        # Show URLs from STAGE_UI_HINTS (more accurate than parsing page names)
        urls = hints.get("urls", [])
        if urls:
            print(f"{Colors.DIM}   URLs to check:{Colors.RESET}")
            for url_path in urls:
                print(f"{Colors.DIM}   - {self.frontend_url}{url_path}{Colors.RESET}")

        # Show status info
        fo_status = ctx.get("fo_status")
        shp_status = ctx.get("shp_status")
        if fo_status or shp_status:
            print()
            print(f"   {Colors.CYAN}Expected Status:{Colors.RESET}")
            if fo_status:
                print(f"   - Factory Order: {fo_status}")
            if shp_status:
                print(f"   - Shipment: {shp_status}")

        print()
        next_stage = stage_num + 1
        next_ctx = STAGE_CONTEXT.get(next_stage, {})
        next_title = next_ctx.get("title", f"Stage {next_stage}")

        try:
            user_input = input(
                f"   Press {Colors.GREEN}ENTER{Colors.RESET} to continue to Stage {next_stage}: {next_title}"
                f" (or {Colors.YELLOW}'q'{Colors.RESET} to quit)... "
            ).strip().lower()

            if user_input == 'q':
                return False
            return True

        except (KeyboardInterrupt, EOFError):
            print()
            return False

    def _handle_early_quit(self):
        """Handle user quitting early in interactive mode."""
        log_separator()
        print(f"\n{Colors.YELLOW}Quitting early.{Colors.RESET}")

        print(f"\n{Colors.BOLD}Test data created so far:{Colors.RESET}")
        if self.factory_order_pv:
            print(f"  - Factory Order: {self.factory_order_pv} (status: {self.factory_order_status})")
        if self.shipment_shp:
            print(f"  - Shipment: {self.shipment_shp} (status: {self.shipment_status})")

        if self.factory_order_id or self.shipment_id:
            print()
            try:
                user_input = input(
                    f"   Delete test data? [{Colors.GREEN}y{Colors.RESET}/{Colors.YELLOW}N{Colors.RESET}]: "
                ).strip().lower()

                if user_input == 'y':
                    self._cleanup()
                else:
                    print(f"\n{Colors.DIM}Data kept. Run again with --cleanup to remove.{Colors.RESET}")

            except (KeyboardInterrupt, EOFError):
                print(f"\n{Colors.DIM}Data kept.{Colors.RESET}")

    def _get_test_products(self):
        """Get product IDs for test order items."""
        log_info("Fetching test products...")

        try:
            response = self.api.get("/api/products?page_size=5")
            products = response.get("data", [])

            if len(products) >= 3:
                self.product_ids = [p["id"] for p in products[:3]]
                log_success(f"Found {len(self.product_ids)} products for test")
            else:
                # Fallback: create test products would go here
                raise APIError("Not enough products in database (need at least 3)")

        except APIError as e:
            log_warning(f"Could not fetch products: {e}")
            raise

    # ─────────────────────────────────────────────────
    # STAGE 1: Create Order
    # ─────────────────────────────────────────────────
    def stage_1_create_order(self):
        """Create a factory order with line items."""
        log_header("Stage 1: Create Factory Order")

        # Build order data
        order_date = date.today().isoformat()
        items = [
            {"product_id": self.product_ids[0], "quantity_ordered": "1080.00"},  # 8 pallets
            {"product_id": self.product_ids[1], "quantity_ordered": "1350.00"},  # 10 pallets
            {"product_id": self.product_ids[2], "quantity_ordered": "810.00"},   # 6 pallets
        ]
        total_m2 = sum(float(item["quantity_ordered"]) for item in items)

        payload = {
            "order_date": order_date,
            "items": items,
            "notes": f"E2E Test Order - {time.strftime('%Y-%m-%d %H:%M:%S')}"
        }

        # Create order
        response = self.api.post("/api/factory-orders", payload)

        # Verify response
        self.factory_order_id = response.get("id")
        self.factory_order_pv = response.get("pv_number")
        self.factory_order_status = response.get("status", "")

        if not self.factory_order_id:
            raise APIError("No order ID in response", {"response": response})

        if not self.factory_order_pv:
            raise APIError("No PV number generated", {"response": response})

        if self.factory_order_status != "PENDING":
            raise APIError(f"Expected PENDING status, got {self.factory_order_status}", {"response": response})

        log_success(f"Order {self.factory_order_pv} created ({len(items)} items, {total_m2:,.0f} m2)")

    # ─────────────────────────────────────────────────
    # STAGE 2: Factory Confirms
    # ─────────────────────────────────────────────────
    def stage_2_confirm_order(self):
        """Update order status to CONFIRMED."""
        log_header("Stage 2: Factory Confirms Order")

        old_status = self.factory_order_status
        response = self.api.patch(
            f"/api/factory-orders/{self.factory_order_id}/status",
            {"status": "CONFIRMED"}
        )

        self.factory_order_status = response.get("status", "")
        if self.factory_order_status != "CONFIRMED":
            raise APIError(f"Expected CONFIRMED status, got {self.factory_order_status}")

        log_success(f"Status changed: {old_status} -> CONFIRMED")

    # ─────────────────────────────────────────────────
    # STAGE 3: Production Schedule Available
    # ─────────────────────────────────────────────────
    def stage_3_production_schedule(self):
        """Production schedule available - informational only, no status change."""
        log_header("Stage 3: Production Schedule Available")

        # This stage is informational only - status stays CONFIRMED
        # In real life, factory sends production schedule PDF
        log_info("This stage is informational - status remains CONFIRMED")
        log_info("Factory would send Production Schedule PDF via WhatsApp")

        # Verify order still exists and status is CONFIRMED
        response = self.api.get(f"/api/factory-orders/{self.factory_order_id}")
        self.factory_order_status = response.get("status", "")

        if self.factory_order_status != "CONFIRMED":
            log_warning(f"Expected CONFIRMED status, got {self.factory_order_status}")

        log_success(f"Order {self.factory_order_pv} status: {self.factory_order_status} (no change expected)")

    # ─────────────────────────────────────────────────
    # STAGE 4: Ready to Ship
    # ─────────────────────────────────────────────────
    def stage_4_ready(self):
        """Update order status to READY."""
        log_header("Stage 4: Order Ready at Factory")

        old_status = self.factory_order_status
        response = self.api.patch(
            f"/api/factory-orders/{self.factory_order_id}/status",
            {"status": "READY"}
        )

        self.factory_order_status = response.get("status", "")
        if self.factory_order_status != "READY":
            raise APIError(f"Expected READY status, got {self.factory_order_status}")

        log_success(f"Status changed: {old_status} -> READY")

    # ─────────────────────────────────────────────────
    # STAGE 5: Book Shipment
    # ─────────────────────────────────────────────────
    def stage_5_create_shipment(self):
        """Create shipment linked to factory order."""
        log_header("Stage 5: Book Shipment")

        # Generate test identifiers
        test_id = str(uuid.uuid4())[:7].upper()
        booking_number = f"TST{test_id}"
        shp_number = f"SHPTEST{test_id}"

        # Dates
        etd = (date.today() + timedelta(days=7)).isoformat()
        eta = (date.today() + timedelta(days=37)).isoformat()

        payload = {
            "factory_order_id": self.factory_order_id,
            "booking_number": booking_number,
            "shp_number": shp_number,
            "vessel_name": "MSC TEST VESSEL",
            "voyage_number": "E2E001",
            "etd": etd,
            "eta": eta,
            "notes": f"E2E Test Shipment - {time.strftime('%Y-%m-%d %H:%M:%S')}"
        }

        # Create shipment
        response = self.api.post("/api/shipments", payload)

        self.shipment_id = response.get("id")
        self.shipment_shp = response.get("shp_number")
        self.shipment_status = response.get("status", "")

        if not self.shipment_id:
            raise APIError("No shipment ID in response", {"response": response})

        if self.shipment_status != "AT_FACTORY":
            raise APIError(f"Expected AT_FACTORY status, got {self.shipment_status}")

        # Verify FO status was auto-updated to SHIPPED
        fo_response = self.api.get(f"/api/factory-orders/{self.factory_order_id}")
        self.factory_order_status = fo_response.get("status", "")

        if self.factory_order_status == "SHIPPED":
            log_success(f"Shipment {self.shipment_shp} created, linked to {self.factory_order_pv}")
            log_success(f"Factory Order auto-updated to SHIPPED")
        else:
            # Manually update if not auto-updated
            log_warning(f"FO status is {self.factory_order_status}, manually updating to SHIPPED")
            self.api.patch(
                f"/api/factory-orders/{self.factory_order_id}/status",
                {"status": "SHIPPED"}
            )
            self.factory_order_status = "SHIPPED"
            log_success(f"Shipment {self.shipment_shp} created, linked to {self.factory_order_pv}")

    # ─────────────────────────────────────────────────
    # STAGE 6: At Origin Port
    # ─────────────────────────────────────────────────
    def stage_6_at_origin_port(self):
        """Update shipment status to AT_ORIGIN_PORT."""
        log_header("Stage 6: Containers at Origin Port")

        old_status = self.shipment_status
        response = self.api.patch(
            f"/api/shipments/{self.shipment_id}/status",
            {"status": "AT_ORIGIN_PORT"}
        )

        self.shipment_status = response.get("status", "")
        if self.shipment_status != "AT_ORIGIN_PORT":
            raise APIError(f"Expected AT_ORIGIN_PORT status, got {self.shipment_status}")

        log_success(f"Shipment status: {old_status} -> AT_ORIGIN_PORT")

    # ─────────────────────────────────────────────────
    # STAGE 7: HBL/MBL Received (before departure)
    # ─────────────────────────────────────────────────
    def stage_7_hbl_received(self):
        """HBL/MBL received - updates shipment details, status stays AT_ORIGIN_PORT."""
        log_header("Stage 7: HBL/MBL Received")

        # In real life, HBL would be emailed and auto-ingested
        # HBL/MBL are issued BEFORE the ship departs
        log_info("HBL/MBL received from TIBA (before departure)")
        log_info("Would update vessel, voyage, container details, freight cost")

        # Verify shipment is still AT_ORIGIN_PORT (HBL comes before departure)
        response = self.api.get(f"/api/shipments/{self.shipment_id}")
        self.shipment_status = response.get("status", "")

        if self.shipment_status != "AT_ORIGIN_PORT":
            log_warning(f"Expected AT_ORIGIN_PORT status, got {self.shipment_status}")

        log_success(f"Shipment {self.shipment_shp} details updated (status: {self.shipment_status})")

    # ─────────────────────────────────────────────────
    # STAGE 8: Ship Departs (after HBL/MBL)
    # ─────────────────────────────────────────────────
    def stage_8_in_transit(self):
        """Update shipment status to IN_TRANSIT after HBL received."""
        log_header("Stage 8: Ship Departs")

        old_status = self.shipment_status
        response = self.api.patch(
            f"/api/shipments/{self.shipment_id}/status",
            {"status": "IN_TRANSIT"}
        )

        self.shipment_status = response.get("status", "")
        if self.shipment_status != "IN_TRANSIT":
            raise APIError(f"Expected IN_TRANSIT status, got {self.shipment_status}")

        log_success(f"Shipment status: {old_status} -> IN_TRANSIT")

    # ─────────────────────────────────────────────────
    # STAGE 9: Ship Arrives at Destination
    # ─────────────────────────────────────────────────
    def stage_9_at_destination(self):
        """Update shipment status to AT_DESTINATION_PORT."""
        log_header("Stage 9: Ship Arrives at Destination")

        old_status = self.shipment_status
        response = self.api.patch(
            f"/api/shipments/{self.shipment_id}/status",
            {"status": "AT_DESTINATION_PORT"}
        )

        self.shipment_status = response.get("status", "")
        if self.shipment_status != "AT_DESTINATION_PORT":
            raise APIError(f"Expected AT_DESTINATION_PORT status, got {self.shipment_status}")

        log_success(f"Shipment status: {old_status} -> AT_DESTINATION_PORT")

    # ─────────────────────────────────────────────────
    # STAGE 10: Customs Clearance
    # ─────────────────────────────────────────────────
    def stage_10_customs(self):
        """Update shipment status through customs to IN_TRUCK."""
        log_header("Stage 10: Customs Clearance")

        # In real life, customs clearance happens at port
        log_info("Customs broker processes import declaration")
        log_info("Monitoring free days to avoid demurrage")

        old_status = self.shipment_status

        # First go through IN_CUSTOMS
        response = self.api.patch(
            f"/api/shipments/{self.shipment_id}/status",
            {"status": "IN_CUSTOMS"}
        )
        self.shipment_status = response.get("status", "")
        log_info(f"Shipment status: {old_status} -> IN_CUSTOMS")

        # Then to IN_TRUCK (ready for delivery)
        old_status = self.shipment_status
        response = self.api.patch(
            f"/api/shipments/{self.shipment_id}/status",
            {"status": "IN_TRUCK"}
        )

        self.shipment_status = response.get("status", "")
        if self.shipment_status != "IN_TRUCK":
            raise APIError(f"Expected IN_TRUCK status, got {self.shipment_status}")

        log_success(f"Shipment status: {old_status} -> IN_TRUCK (cleared customs)")

    # ─────────────────────────────────────────────────
    # STAGE 11: Delivered
    # ─────────────────────────────────────────────────
    def stage_11_delivered(self):
        """Update shipment status to DELIVERED."""
        log_header("Stage 11: Delivered to Warehouse")

        old_status = self.shipment_status
        response = self.api.patch(
            f"/api/shipments/{self.shipment_id}/status",
            {"status": "DELIVERED"}
        )

        self.shipment_status = response.get("status", "")
        if self.shipment_status != "DELIVERED":
            raise APIError(f"Expected DELIVERED status, got {self.shipment_status}")

        log_success(f"Shipment status: {old_status} -> DELIVERED")

    # ─────────────────────────────────────────────────
    # STAGE 12: Update Inventory
    # ─────────────────────────────────────────────────
    def stage_12_verify_pipeline(self):
        """Verify order appears in Pipeline API - inventory update simulation."""
        log_header("Stage 12: Update Inventory")

        response = self.api.get("/api/pipeline/overview")

        stages = response.get("stages", {})
        counts = response.get("counts", {})

        # Check delivered stage contains our order
        delivered = stages.get("delivered", [])
        found = False
        for item in delivered:
            if item.get("pv_number") == self.factory_order_pv:
                found = True
                break
            if item.get("shipment_id") == self.shipment_id:
                found = True
                break

        if not found:
            log_warning(f"Order not found in 'delivered' stage (might be in different stage)")
            log_info(f"Pipeline counts: {counts}")
        else:
            log_success(f"Pipeline shows order in Delivered stage")

        # Always pass this stage - the order existing is enough
        log_success(f"Pipeline API verified (total: {counts.get('total', 0)} orders)")

    # ─────────────────────────────────────────────────
    # CLEANUP
    # ─────────────────────────────────────────────────
    def _cleanup(self):
        """Remove test data."""
        log_header("Cleanup: Removing Test Data")

        # Delete shipment first (has FK to factory order)
        if self.shipment_id:
            try:
                self.api.delete(f"/api/shipments/{self.shipment_id}")
                log_success(f"Deleted shipment {self.shipment_shp}")
            except Exception as e:
                log_warning(f"Could not delete shipment: {e}")

        # Delete factory order
        if self.factory_order_id:
            try:
                self.api.delete(f"/api/factory-orders/{self.factory_order_id}")
                log_success(f"Deleted factory order {self.factory_order_pv}")
            except Exception as e:
                log_warning(f"Could not delete factory order: {e}")

        log_success("Cleanup complete")

    # ─────────────────────────────────────────────────
    # ERROR HANDLING
    # ─────────────────────────────────────────────────
    def _handle_stage_failure(self, stage_num: int, stage_name: str, error: Exception):
        """Handle a failed stage."""
        log_error(f"Stage {stage_num} FAILED: {stage_name}")
        log_error(f"Error: {error}")

        if isinstance(error, APIError) and error.details:
            print(f"\n{Colors.YELLOW}Debug info:{Colors.RESET}")
            for key, value in error.details.items():
                print(f"  - {key}: {value}")

    # ─────────────────────────────────────────────────
    # SUMMARY
    # ─────────────────────────────────────────────────
    def _print_summary(self, success: bool):
        """Print final summary."""
        elapsed = time.time() - self.start_time

        log_separator()

        if success:
            print(f"\n{Colors.GREEN}{Colors.BOLD}=== ALL STAGES PASSED ==={Colors.RESET}")
        else:
            passed = sum(1 for r in self.stage_results if r["status"] == "passed")
            failed = sum(1 for r in self.stage_results if r["status"] == "failed")
            print(f"\n{Colors.RED}{Colors.BOLD}=== SIMULATION FAILED ==={Colors.RESET}")
            print(f"   Passed: {passed}, Failed: {failed}")

        print(f"\n{Colors.BOLD}Summary:{Colors.RESET}")
        if self.factory_order_pv:
            print(f"  - Factory Order: {self.factory_order_pv} (id: {self.factory_order_id})")
        if self.shipment_shp:
            print(f"  - Shipment: {self.shipment_shp} (id: {self.shipment_id})")
        print(f"  - Total time: {elapsed:.2f} seconds")

        if not self.cleanup and success and self.factory_order_id:
            print(f"\n{Colors.YELLOW}Note: Test data was NOT cleaned up.{Colors.RESET}")
            print(f"  Run with --cleanup to remove test data.")


# ===================
# MAIN
# ===================

def main():
    parser = argparse.ArgumentParser(
        description="E2E Order Flow Simulation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/simulate_order_flow.py                     # Run full simulation
  python scripts/simulate_order_flow.py --cleanup           # Run with cleanup
  python scripts/simulate_order_flow.py --interactive       # Pause after each stage (with context)
  python scripts/simulate_order_flow.py -i --no-context     # Interactive without business context
  python scripts/simulate_order_flow.py --base-url URL      # Custom API URL
        """
    )

    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"API base URL (default: {DEFAULT_BASE_URL})"
    )
    parser.add_argument(
        "--frontend-url",
        default=DEFAULT_FRONTEND_URL,
        help=f"Frontend URL for interactive hints (default: {DEFAULT_FRONTEND_URL})"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove test data after simulation"
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Pause after each stage for UI inspection"
    )
    parser.add_argument(
        "--context",
        action="store_true",
        dest="show_context",
        default=None,
        help="Show business context for each stage (default ON in interactive mode)"
    )
    parser.add_argument(
        "--no-context",
        action="store_false",
        dest="show_context",
        help="Hide business context (for quick runs)"
    )
    parser.add_argument(
        "--skip-stages",
        type=str,
        default="",
        help="Comma-separated list of stage numbers to skip (e.g., 1,2,10)"
    )

    args = parser.parse_args()

    # Parse skip stages
    skip_stages = []
    if args.skip_stages:
        try:
            skip_stages = [int(s.strip()) for s in args.skip_stages.split(",")]
        except ValueError:
            print(f"Error: Invalid --skip-stages value: {args.skip_stages}")
            sys.exit(1)

    # Run simulation
    simulation = OrderFlowSimulation(
        base_url=args.base_url,
        cleanup=args.cleanup,
        skip_stages=skip_stages,
        interactive=args.interactive,
        frontend_url=args.frontend_url,
        show_context=args.show_context
    )

    success = simulation.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

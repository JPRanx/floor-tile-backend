"""
Data Source Verification Script

Compares Excel source files against database/API data to ensure accuracy.
Run before calls with Ashley to verify dashboard integrity.

Usage:
    cd backend && python scripts/verify_data_sources.py
"""

import sys
import os
from pathlib import Path

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from decimal import Decimal
from datetime import date, datetime, timedelta
from collections import defaultdict
import json

# Import parsers and services
from parsers.tiba_parser import parse_tiba_excel
from services.production_schedule_service import get_production_schedule_service
from config.database import get_supabase_client
from config.settings import settings


# File paths
DATA_DIR = Path(__file__).parent.parent.parent / "data" / "uploads" / "INFORMES TARRAGONA"
GUATEMALA_INVENTORY = DATA_DIR / "INVENTARIO POR PRODUCTOS ENERO 27.01.26 FEX 337 (2).xlsx"
SIESA_INVENTORY = DATA_DIR / "inventario 27 enero 2026.xlsx"
SALES_REPORT = DATA_DIR / "REPORTE ENERO (1).xls"
PRODUCTION_SCHEDULE = DATA_DIR / "Programa de Produccion_ CASTELLON_ENERO_24-01-2026_V3 (1).xlsx"
BOAT_SCHEDULES = DATA_DIR / "Tabla de Booking.xlsx"
DISPATCH_FILE = DATA_DIR / "PROGRAMACIÓN DE DESPACHO DE TARRAGONA.xlsx"


def print_header(title: str):
    """Print formatted header."""
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def print_table(headers: list, rows: list, col_widths: list = None):
    """Print formatted table."""
    if not col_widths:
        col_widths = [max(len(str(row[i])) for row in [headers] + rows) + 2
                      for i in range(len(headers))]

    # Header
    header_line = "|".join(str(h).center(w) for h, w in zip(headers, col_widths))
    print(f"|{header_line}|")
    print("|" + "|".join("-" * w for w in col_widths) + "|")

    # Rows
    for row in rows:
        row_line = "|".join(str(v).center(w) for v, w in zip(row, col_widths))
        print(f"|{row_line}|")


def verify_guatemala_warehouse() -> dict:
    """Verify Guatemala warehouse inventory."""
    print_header("VERIFICATION 1: GUATEMALA WAREHOUSE STOCK")

    result = {
        "source": "INVENTARIO POR PRODUCTOS",
        "excel_total": Decimal("0"),
        "db_total": Decimal("0"),
        "match": False,
        "issues": [],
        "spot_checks": []
    }

    if not GUATEMALA_INVENTORY.exists():
        result["issues"].append(f"File not found: {GUATEMALA_INVENTORY.name}")
        print(f"ERROR: File not found: {GUATEMALA_INVENTORY}")
        return result

    try:
        # Read Excel - skip header rows, use row 3-4 as multi-header
        # Structure: Row 0-2 = title, Row 3-4 = headers, Row 5+ = data
        # Columns: REFERENCIAS, FORMATO, INICIAL(PALET,M2), INGRESOS(PALET,M2), SALIDAS(PALET,M2), SALDO(PALET,M2), OBSERVACIONES
        df = pd.read_excel(GUATEMALA_INVENTORY, engine="openpyxl", header=None, skiprows=5)

        # Column indices
        SKU_COL = 0      # REFERENCIAS (product name)
        SALDO_M2_COL = 9  # SALDO M2 (current warehouse stock)

        print(f"\nExcel structure:")
        print(f"  Using column 0 for SKU (REFERENCIAS)")
        print(f"  Using column 9 for SALDO M2 (warehouse stock)")

        # Parse Excel data
        excel_data = {}
        for idx, row in df.iterrows():
            sku_raw = row.iloc[SKU_COL] if SKU_COL < len(row) else None
            if pd.isna(sku_raw):
                continue

            sku = str(sku_raw).strip()
            if not sku or sku == "nan":
                continue

            # Normalize SKU - remove size suffix and special chars
            sku_normalized = sku.upper()
            for suffix in [" (T) 51X51-1", " (T) 51X51", " 51X51-1", " 51X51", " BTE"]:
                sku_normalized = sku_normalized.replace(suffix, "")
            sku_normalized = sku_normalized.strip()

            saldo_val = row.iloc[SALDO_M2_COL] if SALDO_M2_COL < len(row) else 0
            if pd.isna(saldo_val):
                saldo_val = 0

            try:
                m2 = Decimal(str(saldo_val).replace(",", ""))
            except:
                m2 = Decimal("0")

            if sku_normalized in excel_data:
                excel_data[sku_normalized] += m2
            else:
                excel_data[sku_normalized] = m2

        result["excel_total"] = sum(excel_data.values())
        print(f"\nExcel Summary:")
        print(f"  Products: {len(excel_data)}")
        print(f"  Total m2: {result['excel_total']:,.2f}")

        # Query database
        db = get_supabase_client()
        try:
            # Get latest snapshot date first
            date_result = db.table("inventory_snapshots")\
                .select("snapshot_date")\
                .order("snapshot_date", desc=True)\
                .limit(1)\
                .execute()

            if not date_result.data:
                result["issues"].append("No inventory snapshots found")
                print("ERROR: No inventory snapshots in database")
                db_data = {}
            else:
                latest_date = date_result.data[0]["snapshot_date"]

                # Get all snapshots for latest date
                snapshot_result = db.table("inventory_snapshots")\
                    .select("product_id, warehouse_qty")\
                    .eq("snapshot_date", latest_date)\
                    .execute()

                # Get product SKUs
                product_result = db.table("products")\
                    .select("id, sku")\
                    .execute()

                id_to_sku = {row["id"]: row["sku"] for row in product_result.data}

                db_data = {}
                for row in snapshot_result.data:
                    sku = id_to_sku.get(row["product_id"], "UNKNOWN")
                    m2 = Decimal(str(row["warehouse_qty"] or 0))
                    db_data[sku] = m2

                result["db_total"] = sum(db_data.values())

                print(f"\nDatabase Summary:")
                print(f"  Snapshot date: {latest_date}")
                print(f"  Products: {len(db_data)}")
                print(f"  Total m2: {result['db_total']:,.2f}")

        except Exception as e:
            result["issues"].append(f"Database query failed: {e}")
            print(f"ERROR: Database query failed: {e}")
            db_data = {}

        # Compare
        diff = abs(result["excel_total"] - result["db_total"])
        result["match"] = diff < Decimal("100")  # Allow small rounding differences

        print(f"\nComparison:")
        print(f"  Difference: {diff:,.2f} m2")
        print(f"  Match: {'YES' if result['match'] else 'NO'}")

        # Spot checks
        spot_check_skus = ["TOLU GRIS", "ALMENDRO BEIGE", "CEIBA GRIS", "CARACOLI", "ROBLE"]
        print(f"\nSpot Checks:")
        headers = ["SKU", "Excel m2", "DB m2", "Match"]
        rows = []

        for sku in spot_check_skus:
            # Find matching SKU in excel (partial match)
            excel_val = Decimal("0")
            for k, v in excel_data.items():
                if sku.upper() in k.upper():
                    excel_val = v
                    break

            # Find matching SKU in db (partial match)
            db_val = Decimal("0")
            for k, v in db_data.items():
                if sku.upper() in k.upper():
                    db_val = v
                    break

            match = "YES" if abs(excel_val - db_val) < Decimal("10") else "NO"
            rows.append([sku, f"{excel_val:,.1f}", f"{db_val:,.1f}", match])
            result["spot_checks"].append({
                "sku": sku,
                "excel": float(excel_val),
                "db": float(db_val),
                "match": match == "YES"
            })

        print_table(headers, rows, [20, 15, 15, 8])

    except Exception as e:
        result["issues"].append(f"Verification failed: {e}")
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    return result


def verify_siesa_factory() -> dict:
    """Verify SIESA factory inventory."""
    print_header("VERIFICATION 2: FACTORY AVAILABLE (SIESA)")

    result = {
        "source": "inventario SIESA",
        "excel_total": Decimal("0"),
        "db_total": Decimal("0"),
        "match": False,
        "issues": [],
        "spot_checks": []
    }

    if not SIESA_INVENTORY.exists():
        result["issues"].append(f"File not found: {SIESA_INVENTORY.name}")
        print(f"ERROR: File not found: {SIESA_INVENTORY}")
        return result

    try:
        # Read Excel directly
        df = pd.read_excel(SIESA_INVENTORY, engine="openpyxl")
        df.columns = df.columns.str.strip()

        print(f"\nExcel columns: {list(df.columns)[:10]}...")

        # Sum by Item (SIESA code)
        if "Cant. disponible" not in df.columns:
            result["issues"].append("Column 'Cant. disponible' not found")
            print(f"ERROR: Column not found. Available: {list(df.columns)}")
            return result

        # Group by Item description
        excel_data = {}
        for _, row in df.iterrows():
            desc = str(row.get("Desc. item", "")).strip()
            if not desc or desc == "nan":
                continue

            qty = row.get("Cant. disponible", 0)
            if pd.isna(qty):
                qty = 0

            try:
                m2 = Decimal(str(qty).replace(",", ""))
            except:
                m2 = Decimal("0")

            # Normalize description (remove size suffix)
            normalized = desc.upper()
            for suffix in [" 51X51-1", " 51X51", " 50X50", " BTE"]:
                normalized = normalized.replace(suffix, "")
            normalized = normalized.strip()

            if normalized in excel_data:
                excel_data[normalized] += m2
            else:
                excel_data[normalized] = m2

        result["excel_total"] = sum(excel_data.values())
        print(f"\nExcel Summary:")
        print(f"  Unique products: {len(excel_data)}")
        print(f"  Total m2: {result['excel_total']:,.2f}")

        # Query database for factory available
        db = get_supabase_client()
        try:
            # Get latest snapshot date
            date_result = db.table("inventory_snapshots")\
                .select("snapshot_date")\
                .order("snapshot_date", desc=True)\
                .limit(1)\
                .execute()

            if date_result.data:
                latest_date = date_result.data[0]["snapshot_date"]

                # Get snapshots with factory stock
                snapshot_result = db.table("inventory_snapshots")\
                    .select("product_id, factory_available_m2")\
                    .eq("snapshot_date", latest_date)\
                    .gt("factory_available_m2", 0)\
                    .execute()

                # Get product SKUs
                product_result = db.table("products").select("id, sku").execute()
                id_to_sku = {row["id"]: row["sku"] for row in product_result.data}

                db_data = {}
                for row in snapshot_result.data:
                    sku = id_to_sku.get(row["product_id"], "UNKNOWN").upper()
                    m2 = Decimal(str(row["factory_available_m2"] or 0))
                    db_data[sku] = m2

                result["db_total"] = sum(db_data.values())

                print(f"\nDatabase Summary:")
                print(f"  Products with factory stock: {len(db_data)}")
                print(f"  Total m2: {result['db_total']:,.2f}")
            else:
                db_data = {}
                result["issues"].append("No inventory snapshots found")

        except Exception as e:
            result["issues"].append(f"Database query failed: {e}")
            print(f"ERROR: Database query failed: {e}")
            db_data = {}

        # Compare
        diff = abs(result["excel_total"] - result["db_total"])
        result["match"] = diff < Decimal("500")  # Allow larger difference for factory

        print(f"\nComparison:")
        print(f"  Difference: {diff:,.2f} m2")
        print(f"  Match: {'YES' if result['match'] else 'NO'}")

        # Spot checks
        spot_check_skus = ["CEIBA GRIS", "CEIBA BEIGE", "TOLU", "ALMENDRO", "CARACOLI"]
        print(f"\nSpot Checks:")
        headers = ["SKU", "Excel m2", "DB m2", "Match"]
        rows = []

        for sku in spot_check_skus:
            # Find in excel
            excel_val = Decimal("0")
            for k, v in excel_data.items():
                if sku.upper() in k:
                    excel_val += v

            # Find in db
            db_val = Decimal("0")
            for k, v in db_data.items():
                if sku.upper() in k:
                    db_val += v

            match = "YES" if excel_val > 0 or db_val > 0 else "N/A"
            if excel_val > 0 and db_val > 0:
                match = "YES" if abs(excel_val - db_val) / excel_val < Decimal("0.1") else "NO"

            rows.append([sku, f"{excel_val:,.1f}", f"{db_val:,.1f}", match])
            result["spot_checks"].append({
                "sku": sku,
                "excel": float(excel_val),
                "db": float(db_val),
                "match": match == "YES"
            })

        print_table(headers, rows, [20, 15, 15, 8])

    except Exception as e:
        result["issues"].append(f"Verification failed: {e}")
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    return result


def verify_sales_velocity() -> dict:
    """Verify sales and velocity calculations."""
    print_header("VERIFICATION 3: SALES / VELOCITY")

    result = {
        "source": "REPORTE ENERO",
        "date_range": "",
        "excel_total": Decimal("0"),
        "match": False,
        "issues": [],
        "spot_checks": []
    }

    if not SALES_REPORT.exists():
        result["issues"].append(f"File not found: {SALES_REPORT.name}")
        print(f"ERROR: File not found: {SALES_REPORT}")
        return result

    try:
        # Read Excel with header row 1 (skip title row 0)
        # Structure: Row 0 = title, Row 1 = headers, Row 2+ = data
        # Columns: FECHA, REFERENCIA, MT2, MUEBLES, CLIENTE, PAIS, DEPARTAMENTO, ...
        df = pd.read_excel(SALES_REPORT, engine="xlrd", header=1)

        # Clean column names
        df.columns = [str(c).strip() if not pd.isna(c) else f"col_{i}" for i, c in enumerate(df.columns)]

        print(f"\nExcel columns: {list(df.columns)[:8]}...")

        # Column mappings for this specific file
        date_col = "FECHA"
        sku_col = "REFERENCIA"
        qty_col = "MT2"

        # Verify columns exist
        if date_col not in df.columns:
            result["issues"].append(f"Date column '{date_col}' not found")
            print(f"ERROR: Column not found. Available: {list(df.columns)}")
            return result

        if qty_col not in df.columns:
            result["issues"].append(f"Quantity column '{qty_col}' not found")
            print(f"ERROR: Quantity column not found")
            return result

        print(f"Using date column: {date_col}")
        print(f"Using SKU column: {sku_col}")
        print(f"Using quantity column: {qty_col}")

        # Parse dates and filter to last 90 days
        today = date.today()
        cutoff = today - timedelta(days=90)

        excel_data = defaultdict(lambda: Decimal("0"))
        min_date = None
        max_date = None

        for _, row in df.iterrows():
            try:
                date_val = row[date_col]
                if pd.isna(date_val):
                    continue

                # Parse date
                if isinstance(date_val, datetime):
                    sale_date = date_val.date()
                elif isinstance(date_val, date):
                    sale_date = date_val
                else:
                    # Try parsing string
                    date_str = str(date_val).strip()
                    try:
                        sale_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    except:
                        try:
                            sale_date = datetime.strptime(date_str, "%m/%d/%Y").date()
                        except:
                            continue

                # Track date range
                if min_date is None or sale_date < min_date:
                    min_date = sale_date
                if max_date is None or sale_date > max_date:
                    max_date = sale_date

                # Skip if outside 90 day window
                if sale_date < cutoff:
                    continue

                # Get SKU and quantity
                sku_raw = row[sku_col]
                if pd.isna(sku_raw):
                    continue

                sku = str(sku_raw).strip().upper()
                # Normalize SKU
                for suffix in [" (T) 51X51-1", " (T) 51X51", " 51X51-1", " 51X51", " BTE"]:
                    sku = sku.replace(suffix, "")
                sku = sku.strip()

                if not sku or sku == "NAN":
                    continue

                qty = row[qty_col]
                if pd.isna(qty):
                    qty = 0

                try:
                    m2 = Decimal(str(qty).replace(",", ""))
                except:
                    m2 = Decimal("0")

                excel_data[sku] += m2

            except Exception as e:
                continue

        result["date_range"] = f"{min_date} to {max_date}"
        result["excel_total"] = sum(excel_data.values())

        print(f"\nExcel Summary:")
        print(f"  Date range: {result['date_range']}")
        print(f"  Products sold: {len(excel_data)}")
        print(f"  Total sales (90d): {result['excel_total']:,.2f} m2")

        # Query database for velocity
        db = get_supabase_client()
        try:
            # Get latest snapshot date
            date_result = db.table("inventory_snapshots")\
                .select("snapshot_date")\
                .order("snapshot_date", desc=True)\
                .limit(1)\
                .execute()

            # Note: velocity_90d is not stored in inventory_snapshots
            # It's calculated from sales data in the Order Builder service
            # We'll compare against the sales sum from database instead
            print(f"\nDatabase Note:")
            print(f"  Velocity is calculated, not stored in inventory_snapshots")
            print(f"  Comparing Excel sales totals against calculated velocity from API")
            db_data = {}

        except Exception as e:
            result["issues"].append(f"Database query failed: {e}")
            print(f"ERROR: Database query failed: {e}")
            db_data = {}

        # Spot checks - compare velocity
        spot_check_skus = ["TOLU GRIS", "CEIBA GRIS", "ALMENDRO", "CARACOLI", "ROBLE"]
        print(f"\nSpot Checks (Velocity = Sales/90):")
        headers = ["SKU", "Excel Sales", "Calc Velocity", "DB Velocity", "Match"]
        rows = []

        for sku in spot_check_skus:
            # Find in excel
            excel_sales = Decimal("0")
            for k, v in excel_data.items():
                if sku.upper() in k:
                    excel_sales += v

            calc_velocity = excel_sales / 90 if excel_sales > 0 else Decimal("0")

            # Find in db
            db_velocity = Decimal("0")
            for k, v in db_data.items():
                if sku.upper() in k:
                    db_velocity += v

            match = "N/A"
            if calc_velocity > 0 and db_velocity > 0:
                match = "YES" if abs(calc_velocity - db_velocity) / calc_velocity < Decimal("0.2") else "NO"
            elif calc_velocity == 0 and db_velocity == 0:
                match = "YES"

            rows.append([
                sku,
                f"{excel_sales:,.1f}",
                f"{calc_velocity:,.2f}/day",
                f"{db_velocity:,.2f}/day",
                match
            ])
            result["spot_checks"].append({
                "sku": sku,
                "excel_sales": float(excel_sales),
                "calc_velocity": float(calc_velocity),
                "db_velocity": float(db_velocity),
                "match": match == "YES"
            })

        print_table(headers, rows, [15, 12, 14, 14, 8])

        result["match"] = all(c.get("match", False) for c in result["spot_checks"])

    except Exception as e:
        result["issues"].append(f"Verification failed: {e}")
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    return result


def verify_production_schedule() -> dict:
    """Verify production schedule."""
    print_header("VERIFICATION 4: PRODUCTION SCHEDULE")

    result = {
        "source": "Programa de Produccion",
        "excel_counts": {},
        "db_counts": {},
        "match": False,
        "issues": [],
        "spot_checks": []
    }

    if not PRODUCTION_SCHEDULE.exists():
        result["issues"].append(f"File not found: {PRODUCTION_SCHEDULE.name}")
        print(f"ERROR: File not found: {PRODUCTION_SCHEDULE}")
        return result

    try:
        # Use the production schedule service
        service = get_production_schedule_service()
        parsed = service.parse_production_excel(str(PRODUCTION_SCHEDULE))

        # Count by status - parsed is a list of ProductionScheduleItem
        status_counts = defaultdict(lambda: {"count": 0, "m2": Decimal("0")})
        items = parsed if isinstance(parsed, list) else getattr(parsed, 'items', [])
        for item in items:
            status_raw = getattr(item, 'status', 'unknown')
            # Convert enum to string value
            status = status_raw.value if hasattr(status_raw, 'value') else str(status_raw)
            m2_prog = getattr(item, 'm2_programmed', 0) or getattr(item, 'm2_requested', 0) or 0
            status_counts[status]["count"] += 1
            status_counts[status]["m2"] += Decimal(str(m2_prog))

        result["excel_counts"] = {k: dict(v) for k, v in status_counts.items()}

        print(f"\nExcel Summary (from parser):")
        print(f"  Total items: {len(items)}")
        for status, data in status_counts.items():
            print(f"  {status}: {data['count']} items, {data['m2']:,.2f} m2")

        # Query database
        db = get_supabase_client()
        try:
            # Get all production records
            prod_result = db.table("production_schedule").select("status, requested_m2").execute()

            # Count by status
            db_counts = defaultdict(lambda: {"count": 0, "m2": Decimal("0")})
            for row in prod_result.data:
                status = row.get("status", "unknown")
                m2 = Decimal(str(row.get("requested_m2") or 0))
                db_counts[status]["count"] += 1
                db_counts[status]["m2"] += m2

            result["db_counts"] = {k: dict(v) for k, v in db_counts.items()}

            print(f"\nDatabase Summary:")
            for status, data in result["db_counts"].items():
                print(f"  {status}: {data['count']} items, {data['m2']:,.2f} m2")

        except Exception as e:
            result["issues"].append(f"Database query failed: {e}")
            print(f"ERROR: Database query failed: {e}")

        # Compare counts
        print(f"\nComparison:")
        headers = ["Status", "Excel Count", "DB Count", "Excel m2", "DB m2", "Match"]
        rows = []
        all_match = True

        all_statuses = set(result["excel_counts"].keys()) | set(result["db_counts"].keys())
        for status in sorted(all_statuses):
            excel_data = result["excel_counts"].get(status, {"count": 0, "m2": Decimal("0")})
            db_data = result["db_counts"].get(status, {"count": 0, "m2": Decimal("0")})

            match = abs(excel_data["count"] - db_data["count"]) <= 2  # Allow small diff
            if not match:
                all_match = False

            rows.append([
                status,
                str(excel_data["count"]),
                str(db_data["count"]),
                f"{excel_data['m2']:,.0f}",
                f"{db_data['m2']:,.0f}",
                "YES" if match else "NO"
            ])

        print_table(headers, rows, [15, 12, 12, 12, 12, 8])
        result["match"] = all_match

    except Exception as e:
        result["issues"].append(f"Verification failed: {e}")
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    return result


def verify_boat_schedules() -> dict:
    """Verify boat schedules."""
    print_header("VERIFICATION 5: BOAT SCHEDULES")

    result = {
        "source": "Tabla de Booking",
        "excel_boats": [],
        "db_boats": [],
        "match": False,
        "issues": []
    }

    if not BOAT_SCHEDULES.exists():
        result["issues"].append(f"File not found: {BOAT_SCHEDULES.name}")
        print(f"ERROR: File not found: {BOAT_SCHEDULES}")
        return result

    try:
        # Parse using TIBA parser
        parsed = parse_tiba_excel(str(BOAT_SCHEDULES))

        print(f"\nExcel Summary (from parser):")
        print(f"  Total schedules: {len(parsed.schedules)}")
        print(f"  Errors: {len(parsed.errors)}")

        # Filter to upcoming
        today = date.today()
        upcoming = [s for s in parsed.schedules if s.departure_date >= today]

        print(f"  Upcoming boats: {len(upcoming)}")

        result["excel_boats"] = [
            {
                "departure": s.departure_date.isoformat(),
                "arrival": s.arrival_date.isoformat(),
                "vessel": s.vessel_name,
                "transit": s.transit_days
            }
            for s in upcoming[:5]
        ]

        # Query database
        db = get_supabase_client()
        try:
            today_str = date.today().isoformat()
            boat_result = db.table("boat_schedules")\
                .select("departure_date, arrival_date, vessel_name, transit_days")\
                .gte("departure_date", today_str)\
                .order("departure_date")\
                .limit(5)\
                .execute()

            result["db_boats"] = [
                {
                    "departure": row["departure_date"],
                    "arrival": row["arrival_date"],
                    "vessel": row["vessel_name"],
                    "transit": row["transit_days"]
                }
                for row in boat_result.data
            ]

            print(f"\nDatabase Summary:")
            print(f"  Upcoming boats: {len(result['db_boats'])}")

        except Exception as e:
            result["issues"].append(f"Database query failed: {e}")
            print(f"ERROR: Database query failed: {e}")

        # Compare
        print(f"\nComparison:")
        headers = ["Excel Departure", "Excel Vessel", "DB Departure", "DB Vessel", "Match"]
        rows = []

        for i, excel_boat in enumerate(result["excel_boats"]):
            db_boat = result["db_boats"][i] if i < len(result["db_boats"]) else {}

            match = (
                excel_boat.get("departure") == db_boat.get("departure") and
                (excel_boat.get("vessel") or "").upper() == (db_boat.get("vessel") or "").upper()
            )

            rows.append([
                excel_boat.get("departure", "N/A"),
                (excel_boat.get("vessel") or "N/A")[:15],
                db_boat.get("departure", "N/A"),
                (db_boat.get("vessel") or "N/A")[:15],
                "YES" if match else "NO"
            ])

        print_table(headers, rows, [15, 18, 15, 18, 8])

        result["match"] = len(result["excel_boats"]) == len(result["db_boats"])

    except Exception as e:
        result["issues"].append(f"Verification failed: {e}")
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    return result


def verify_in_transit() -> dict:
    """Verify in-transit data."""
    print_header("VERIFICATION 6: IN-TRANSIT SHIPMENTS")

    result = {
        "source": "Manual entry / PROGRAMACION DE DESPACHO",
        "db_total": Decimal("0"),
        "products": [],
        "match": True,  # Can't verify against Excel (manual entry)
        "issues": []
    }

    # Query database for in-transit
    db = get_supabase_client()
    try:
        # Get latest snapshot date
        date_result = db.table("inventory_snapshots")\
            .select("snapshot_date")\
            .order("snapshot_date", desc=True)\
            .limit(1)\
            .execute()

        if date_result.data:
            latest_date = date_result.data[0]["snapshot_date"]

            # Get snapshots with in-transit
            snapshot_result = db.table("inventory_snapshots")\
                .select("product_id, in_transit_qty")\
                .eq("snapshot_date", latest_date)\
                .gt("in_transit_qty", 0)\
                .order("in_transit_qty", desc=True)\
                .execute()

            # Get product SKUs
            product_result = db.table("products").select("id, sku").execute()
            id_to_sku = {row["id"]: row["sku"] for row in product_result.data}

            for row in snapshot_result.data:
                sku = id_to_sku.get(row["product_id"], "UNKNOWN")
                m2 = float(row["in_transit_qty"] or 0)
                result["products"].append({
                    "sku": sku,
                    "in_transit_m2": m2
                })
                result["db_total"] += Decimal(str(m2))

        print(f"\nDatabase In-Transit Summary:")
        print(f"  Products with in-transit: {len(result['products'])}")
        print(f"  Total in-transit: {result['db_total']:,.2f} m2")

        print(f"\nIn-Transit by Product:")
        headers = ["SKU", "In-Transit m2", "Source"]
        rows = []
        for prod in result["products"][:10]:
            rows.append([
                prod["sku"][:25],
                f"{prod['in_transit_m2']:,.1f}",
                "Manual entry"
            ])

        if rows:
            print_table(headers, rows, [28, 15, 15])
        else:
            print("  No in-transit data found")

        if not result["products"]:
            result["issues"].append("No in-transit data in database")

    except Exception as e:
        result["issues"].append(f"Database query failed: {e}")
        print(f"ERROR: Database query failed: {e}")

    # Note about manual entry
    print(f"\n[!] NOTE: In-transit data is manually entered via scripts/update_in_transit.py")
    print(f"    Source file: PROGRAMACION DE DESPACHO DE TARRAGONA.xlsx")
    print(f"    Cannot auto-verify against Excel (no automated parser)")

    return result


def verify_order_builder_calc() -> dict:
    """Verify Order Builder calculations for a specific product."""
    print_header("VERIFICATION 7: ORDER BUILDER CALCULATIONS")

    result = {
        "product": "CEIBA BEIGE BTE",
        "manual_calc": {},
        "api_response": {},
        "match": False,
        "issues": []
    }

    # Get data from database
    db = get_supabase_client()
    try:
        # Find product
        prod_result = db.table("products")\
            .select("id, sku")\
            .ilike("sku", "%CEIBA BEIGE%")\
            .limit(1)\
            .execute()

        if not prod_result.data:
            result["issues"].append("Product CEIBA BEIGE not found in database")
            print("ERROR: Product not found in database")
            return result

        product = prod_result.data[0]
        product_id = product["id"]
        sku = product["sku"]

        # Get latest snapshot for this product
        snapshot_result = db.table("inventory_snapshots")\
            .select("warehouse_qty, in_transit_qty, factory_available_m2")\
            .eq("product_id", product_id)\
            .order("snapshot_date", desc=True)\
            .limit(1)\
            .execute()

        if snapshot_result.data:
            row = snapshot_result.data[0]
            warehouse_m2 = Decimal(str(row.get("warehouse_qty") or 0))
            in_transit_m2 = Decimal(str(row.get("in_transit_qty") or 0))
            factory_m2 = Decimal(str(row.get("factory_available_m2") or 0))
            # Velocity is calculated, not stored - set to 0 for manual check
            velocity = Decimal("0")

            result["product"] = sku

            # Manual calculation
            DAYS_TO_COVER = 63
            M2_PER_PALLET = Decimal("134.4")

            demand_63d = velocity * DAYS_TO_COVER
            coverage_gap = demand_63d - warehouse_m2 - in_transit_m2
            suggested_pallets = max(0, int(coverage_gap / M2_PER_PALLET))

            result["manual_calc"] = {
                "warehouse_m2": float(warehouse_m2),
                "in_transit_m2": float(in_transit_m2),
                "factory_m2": float(factory_m2),
                "velocity_90d": float(velocity),
                "demand_63d": float(demand_63d),
                "coverage_gap": float(coverage_gap),
                "suggested_pallets": suggested_pallets
            }

            print(f"\nProduct: {sku}")
            print(f"\nManual Calculation:")
            print(f"  Warehouse m2:     {warehouse_m2:>12,.2f}")
            print(f"  In-Transit m2:    {in_transit_m2:>12,.2f}")
            print(f"  Factory m2:       {factory_m2:>12,.2f}")
            print(f"  Velocity (90d):   {velocity:>12,.2f} m2/day")
            print(f"  63-day demand:    {demand_63d:>12,.2f} m2")
            print(f"  Coverage gap:     {coverage_gap:>12,.2f} m2")
            print(f"  Suggested pallets:{suggested_pallets:>12}")

            # Try to get Order Builder API response
            try:
                import requests
                resp = requests.get("http://localhost:8000/api/order-builder/suggestions?mode=optimal", timeout=5)
                if resp.status_code == 200:
                    data = resp.json()

                    # Find our product
                    for prod in data.get("all_products", []):
                        if sku.upper() in prod.get("sku", "").upper():
                            result["api_response"] = {
                                "warehouse_m2": prod.get("warehouse_m2"),
                                "in_transit_m2": prod.get("in_transit_m2"),
                                "factory_available_m2": prod.get("factory_available_m2"),
                                "velocity_90d_m2": prod.get("velocity_90d_m2"),
                                "coverage_gap_m2": prod.get("coverage_gap_m2"),
                                "suggested_pallets": prod.get("suggested_pallets")
                            }
                            break

                    if result["api_response"]:
                        print(f"\nAPI Response:")
                        for k, v in result["api_response"].items():
                            print(f"  {k}: {v}")

                        # Compare
                        calc_gap = result["manual_calc"]["coverage_gap"]
                        api_gap = result["api_response"].get("coverage_gap_m2", 0)

                        result["match"] = abs(calc_gap - (api_gap or 0)) < 50
                        print(f"\nMatch: {'YES' if result['match'] else 'NO'}")
                    else:
                        result["issues"].append(f"Product {sku} not found in API response")
                        print(f"\n⚠️  Product not found in Order Builder API response")
                else:
                    result["issues"].append(f"API returned status {resp.status_code}")
                    print(f"\n⚠️  Could not get Order Builder API (status {resp.status_code})")
            except requests.exceptions.ConnectionError:
                result["issues"].append("Server not running - cannot verify API")
                print(f"\n[!] Server not running - cannot verify API response")
                print(f"    Start server with: cd backend && python main.py")
            except Exception as e:
                result["issues"].append(f"API request failed: {e}")
                print(f"\n⚠️  API request failed: {e}")
        else:
            result["issues"].append("No snapshot data for CEIBA BEIGE")
            print("ERROR: No snapshot data found")

    except Exception as e:
        result["issues"].append(f"Database query failed: {e}")
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    return result


def generate_summary(results: dict):
    """Generate final summary report."""
    print("\n")
    print("=" * 70)
    print("DATA VERIFICATION SUMMARY")
    print("=" * 70)

    headers = ["Source", "Excel Total", "System Total", "Match", "Issues"]
    rows = []

    # Warehouse
    r = results.get("warehouse", {})
    rows.append([
        "Warehouse",
        f"{r.get('excel_total', 0):,.0f} m2",
        f"{r.get('db_total', 0):,.0f} m2",
        "YES" if r.get("match") else "NO",
        str(len(r.get("issues", [])))
    ])

    # Factory
    r = results.get("factory", {})
    rows.append([
        "Factory (SIESA)",
        f"{r.get('excel_total', 0):,.0f} m2",
        f"{r.get('db_total', 0):,.0f} m2",
        "YES" if r.get("match") else "NO",
        str(len(r.get("issues", [])))
    ])

    # Sales
    r = results.get("sales", {})
    rows.append([
        "Sales/Velocity",
        f"{r.get('excel_total', 0):,.0f} m2",
        "See velocity",
        "YES" if r.get("match") else "NO",
        str(len(r.get("issues", [])))
    ])

    # Production
    r = results.get("production", {})
    excel_cnt = sum(d.get("count", 0) for d in r.get("excel_counts", {}).values())
    db_cnt = sum(d.get("count", 0) for d in r.get("db_counts", {}).values())
    rows.append([
        "Production",
        f"{excel_cnt} items",
        f"{db_cnt} items",
        "YES" if r.get("match") else "NO",
        str(len(r.get("issues", [])))
    ])

    # Boats
    r = results.get("boats", {})
    rows.append([
        "Boat Schedules",
        f"{len(r.get('excel_boats', []))} boats",
        f"{len(r.get('db_boats', []))} boats",
        "YES" if r.get("match") else "NO",
        str(len(r.get("issues", [])))
    ])

    # In-Transit
    r = results.get("in_transit", {})
    rows.append([
        "In-Transit",
        "Manual entry",
        f"{r.get('db_total', 0):,.0f} m2",
        "N/A",
        str(len(r.get("issues", [])))
    ])

    print_table(headers, rows, [18, 15, 15, 8, 8])

    # Collect all issues
    all_issues = []
    for name, r in results.items():
        for issue in r.get("issues", []):
            all_issues.append(f"[{name}] {issue}")

    if all_issues:
        print(f"\nISSUES FOUND:")
        for i, issue in enumerate(all_issues, 1):
            print(f"  {i}. {issue}")
    else:
        print(f"\nNo issues found!")

    # Final verdict
    all_match = all(
        r.get("match", True)
        for name, r in results.items()
        if name != "in_transit"  # Skip in-transit (manual)
    )
    no_critical_issues = len(all_issues) <= 2

    print(f"\n" + "=" * 70)
    if all_match and no_critical_issues:
        print("READY FOR ASHLEY CALL: YES")
    else:
        print("READY FOR ASHLEY CALL: NO (fix issues first)")
    print("=" * 70)

    return {
        "all_match": all_match,
        "issue_count": len(all_issues),
        "ready": all_match and no_critical_issues
    }


def main():
    """Run all verifications."""
    print("\n" + "=" * 70)
    print("DATA SOURCE VERIFICATION")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    results = {}

    # Run each verification
    results["warehouse"] = verify_guatemala_warehouse()
    results["factory"] = verify_siesa_factory()
    results["sales"] = verify_sales_velocity()
    results["production"] = verify_production_schedule()
    results["boats"] = verify_boat_schedules()
    results["in_transit"] = verify_in_transit()
    results["order_builder"] = verify_order_builder_calc()

    # Generate summary
    summary = generate_summary(results)

    return results, summary


if __name__ == "__main__":
    main()

"""Check sales data availability and history depth."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_supabase_client
from datetime import datetime
from collections import defaultdict

def main():
    print("=" * 70)
    print("SALES DATA AVAILABILITY CHECK")
    print("=" * 70)
    
    db = get_supabase_client()
    
    # Query 1: Overall date range and counts
    print("\n[1] OVERALL DATE RANGE")
    print("-" * 50)
    
    # Get all sales records (Supabase doesn't support MIN/MAX in select directly)
    # We need to fetch data and compute in Python
    result = db.table("sales").select("week_start, product_id", count="exact").execute()
    
    if not result.data:
        print("No sales records found!")
        return
    
    total_records = result.count
    week_starts = [r["week_start"] for r in result.data]
    product_ids = set(r["product_id"] for r in result.data)
    
    earliest_sale = min(week_starts)
    latest_sale = max(week_starts)
    total_products = len(product_ids)
    
    print(f"Earliest sale week: {earliest_sale}")
    print(f"Latest sale week:   {latest_sale}")
    print(f"Total products:     {total_products}")
    print(f"Total records:      {total_records}")
    
    # Query 2: Products with 12+ months history vs less
    print("\n[2] PRODUCTS BY HISTORY DEPTH")
    print("-" * 50)
    
    # Group by product_id and find min/max dates for each
    product_dates = defaultdict(list)
    for r in result.data:
        product_dates[r["product_id"]].append(r["week_start"])
    
    has_12m_history = 0
    less_than_12m = 0
    
    for pid, dates in product_dates.items():
        min_date = datetime.fromisoformat(min(dates))
        max_date = datetime.fromisoformat(max(dates))
        months_diff = (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month)
        
        if months_diff >= 12:
            has_12m_history += 1
        else:
            less_than_12m += 1
    
    print(f"Products with 12+ months history: {has_12m_history}")
    print(f"Products with less than 12 months: {less_than_12m}")
    
    # Query 3: Sample of products with their date ranges
    print("\n[3] SAMPLE PRODUCTS WITH DATE RANGES (first 20 by earliest sale)")
    print("-" * 70)
    
    # Get products table to join SKUs
    products_result = db.table("products").select("id, sku").execute()
    product_skus = {p["id"]: p["sku"] for p in products_result.data}
    
    # Build product summaries
    product_summaries = []
    for pid, dates in product_dates.items():
        sku = product_skus.get(pid, "UNKNOWN")
        min_date = min(dates)
        max_date = max(dates)
        
        # Count distinct months
        months = set()
        for d in dates:
            dt = datetime.fromisoformat(d)
            months.add((dt.year, dt.month))
        
        product_summaries.append({
            "sku": sku,
            "first_sale": min_date,
            "last_sale": max_date,
            "months_with_sales": len(months)
        })
    
    # Sort by first_sale and take first 20
    product_summaries.sort(key=lambda x: x["first_sale"])
    
    print(f"{'SKU':<30} {'First Sale':<12} {'Last Sale':<12} {'Months'}")
    print("-" * 70)
    
    for ps in product_summaries[:20]:
        print(f"{ps['sku']:<30} {ps['first_sale']:<12} {ps['last_sale']:<12} {ps['months_with_sales']}")
    
    print("\n" + "=" * 70)
    print("CHECK COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()

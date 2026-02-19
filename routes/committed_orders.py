"""
Committed orders (pedidos comprometidos) API routes.

Upload Excel files from SIESA ERP with committed order data
(Item | Referencia | Desc. item | Lote | Cant. comprometida | Existencia |
 Cant. disponible | Fecha ultima entrada | Bodega | U.M. | pedido)
and upsert to committed_orders table.

Follows the preview-then-confirm pattern from unfulfilled_demand.py.
"""

from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import date
from io import BytesIO
import hashlib
import structlog
import pandas as pd

from models.committed_orders import (
    CommittedOrderPreview,
    CommittedOrderPreviewRow,
    CommittedOrderConfirmRequest,
    CommittedOrderResponse,
)
from parsers.excel_parser import _normalize_sku_name
from config import get_supabase_client
from services import preview_cache_service
from services.upload_history_service import get_upload_history_service
from exceptions import AppError

router = APIRouter()
logger = structlog.get_logger(__name__)


def _handle_error(e: Exception) -> JSONResponse:
    """Convert exception to JSON response."""
    if isinstance(e, AppError):
        return JSONResponse(
            status_code=e.status_code,
            content=e.to_dict()
        )
    logger.error("unexpected_error", error=str(e), type=type(e).__name__)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred"
            }
        }
    )


def _parse_committed_orders_excel(content: bytes) -> list[dict]:
    """
    Parse committed orders Excel file from SIESA.

    Expected columns: Item | Referencia | Desc. item | Lote |
    Cant. comprometida | Existencia | Cant. disponible |
    Fecha ultima entrada | Bodega | U.M. | pedido

    Returns list of dicts with keys: sku, quantity_committed,
    current_stock, available_qty, warehouse_code, order_reference.
    """
    file_obj = BytesIO(content)

    # Try openpyxl first (xlsx), fall back to xlrd (xls)
    for engine in ("openpyxl", "xlrd"):
        try:
            df = pd.read_excel(file_obj, header=None, engine=engine)
            break
        except Exception:
            file_obj.seek(0)
            continue
    else:
        raise ValueError("No se pudo leer el archivo Excel. Formatos soportados: .xlsx, .xls")

    # Find header row: scan first 5 rows for key column names
    header_row = 0
    for i in range(min(5, len(df))):
        row_text = " ".join(str(v).upper() for v in df.iloc[i] if pd.notna(v))
        if "REFERENCIA" in row_text or "ITEM" in row_text or "COMPROMETIDA" in row_text:
            header_row = i
            break

    # Re-read with header
    file_obj.seek(0)
    df = pd.read_excel(file_obj, header=header_row, engine=engine)
    df.columns = [str(c).upper().strip() for c in df.columns]

    # Map column names (handle variants)
    col_map: dict[str, Optional[str]] = {
        "sku_ref": None,
        "sku_desc": None,
        "quantity": None,
        "stock": None,
        "available": None,
        "warehouse": None,
        "order_ref": None,
    }

    for col in df.columns:
        col_upper = col.upper()
        if col_upper == "REFERENCIA":
            col_map["sku_ref"] = col
        elif "DESC" in col_upper and "ITEM" in col_upper:
            col_map["sku_desc"] = col
        elif "COMPROMETIDA" in col_upper:
            col_map["quantity"] = col
        elif col_upper == "EXISTENCIA":
            col_map["stock"] = col
        elif "DISPONIBLE" in col_upper:
            col_map["available"] = col
        elif col_upper == "BODEGA":
            col_map["warehouse"] = col
        elif col_upper == "PEDIDO":
            col_map["order_ref"] = col

    # Must have at least a quantity column and one SKU source
    if not col_map["quantity"]:
        raise ValueError(
            "Columna faltante: CANT. COMPROMETIDA. "
            "Se espera una columna con 'COMPROMETIDA' en el nombre."
        )

    if not col_map["sku_ref"] and not col_map["sku_desc"]:
        raise ValueError(
            "Columnas faltantes: REFERENCIA o DESC. ITEM. "
            "Se necesita al menos una columna para identificar el producto."
        )

    rows = []
    for _, row in df.iterrows():
        # Get quantity
        qty_val = row[col_map["quantity"]]
        if pd.isna(qty_val):
            continue

        try:
            quantity = float(qty_val)
        except (ValueError, TypeError):
            continue

        if quantity <= 0:
            continue

        # Get SKU: prefer Referencia, fall back to Desc. item
        sku_val = None
        if col_map["sku_ref"]:
            ref_val = row[col_map["sku_ref"]]
            if pd.notna(ref_val) and str(ref_val).strip():
                sku_val = str(ref_val).strip()

        if not sku_val and col_map["sku_desc"]:
            desc_val = row[col_map["sku_desc"]]
            if pd.notna(desc_val) and str(desc_val).strip():
                sku_val = str(desc_val).strip()

        if not sku_val:
            continue

        # Get optional fields
        current_stock = None
        if col_map["stock"]:
            stock_val = row[col_map["stock"]]
            if pd.notna(stock_val):
                try:
                    current_stock = float(stock_val)
                except (ValueError, TypeError):
                    pass

        available_qty = None
        if col_map["available"]:
            avail_val = row[col_map["available"]]
            if pd.notna(avail_val):
                try:
                    available_qty = float(avail_val)
                except (ValueError, TypeError):
                    pass

        warehouse_code = None
        if col_map["warehouse"]:
            wh_val = row[col_map["warehouse"]]
            if pd.notna(wh_val) and str(wh_val).strip():
                warehouse_code = str(wh_val).strip()

        order_reference = None
        if col_map["order_ref"]:
            ord_val = row[col_map["order_ref"]]
            if pd.notna(ord_val) and str(ord_val).strip():
                order_reference = str(ord_val).strip()

        rows.append({
            "sku": sku_val,
            "quantity_committed": round(quantity, 2),
            "current_stock": current_stock,
            "available_qty": available_qty,
            "warehouse_code": warehouse_code,
            "order_reference": order_reference,
        })

    return rows


@router.post("/preview", response_model=CommittedOrderPreview)
async def preview_committed_orders(file: UploadFile = File(...)):
    """
    Parse committed orders Excel from SIESA and return preview.

    Expected columns: Referencia | Desc. item | Cant. comprometida |
    Existencia | Cant. disponible | Bodega | pedido.
    Nothing is saved until /confirm is called.
    """
    try:
        content = await file.read()
        file_hash = hashlib.sha256(content).hexdigest()

        # Check for duplicate upload
        duplicate = get_upload_history_service().check_duplicate(
            "committed_orders", file_hash
        )

        # Parse Excel
        parsed_rows = _parse_committed_orders_excel(content)

        if not parsed_rows:
            raise ValueError("No se encontraron filas validas en el archivo.")

        # Get all active products for matching
        db = get_supabase_client()
        products_result = (
            db.table("products")
            .select("id, sku")
            .eq("active", True)
            .execute()
        )

        # Build normalized SKU -> product_id lookup
        sku_map: dict[str, tuple[str, str]] = {}
        for p in products_result.data or []:
            normalized = _normalize_sku_name(p["sku"])
            sku_map[normalized] = (p["id"], p["sku"])

        # Match parsed rows against products and aggregate by product
        warnings: list[str] = []
        unmatched_skus: set[str] = set()

        # Aggregate by product (sum quantities if same product from different lots)
        # Key: normalized_sku -> aggregated data
        aggregated: dict[str, dict] = {}
        for row in parsed_rows:
            normalized = _normalize_sku_name(row["sku"])
            if normalized in aggregated:
                aggregated[normalized]["quantity_committed"] += row["quantity_committed"]
                # Keep the first non-null values for optional fields
                if row["current_stock"] is not None and aggregated[normalized]["current_stock"] is None:
                    aggregated[normalized]["current_stock"] = row["current_stock"]
                if row["available_qty"] is not None and aggregated[normalized]["available_qty"] is None:
                    aggregated[normalized]["available_qty"] = row["available_qty"]
                if row["warehouse_code"] and not aggregated[normalized]["warehouse_code"]:
                    aggregated[normalized]["warehouse_code"] = row["warehouse_code"]
                if row["order_reference"] and not aggregated[normalized]["order_reference"]:
                    aggregated[normalized]["order_reference"] = row["order_reference"]
            else:
                aggregated[normalized] = {
                    "original_sku": row["sku"],
                    "quantity_committed": row["quantity_committed"],
                    "current_stock": row["current_stock"],
                    "available_qty": row["available_qty"],
                    "warehouse_code": row["warehouse_code"],
                    "order_reference": row["order_reference"],
                }

        # Build preview rows
        preview_rows: list[CommittedOrderPreviewRow] = []
        for normalized_sku, agg in aggregated.items():
            match = sku_map.get(normalized_sku)

            if match:
                product_id, canonical_sku = match
                preview_rows.append(CommittedOrderPreviewRow(
                    sku=canonical_sku,
                    product_id=product_id,
                    quantity_committed=round(agg["quantity_committed"], 2),
                    current_stock=agg["current_stock"],
                    available_qty=agg["available_qty"],
                    warehouse_code=agg["warehouse_code"],
                    order_reference=agg["order_reference"],
                    matched=True,
                ))
            else:
                unmatched_skus.add(agg["original_sku"])
                preview_rows.append(CommittedOrderPreviewRow(
                    sku=agg["original_sku"],
                    product_id=None,
                    quantity_committed=round(agg["quantity_committed"], 2),
                    current_stock=agg["current_stock"],
                    available_qty=agg["available_qty"],
                    warehouse_code=agg["warehouse_code"],
                    order_reference=agg["order_reference"],
                    matched=False,
                ))

        if unmatched_skus:
            warnings.append(
                f"{len(unmatched_skus)} producto(s) no encontrado(s): "
                f"{', '.join(sorted(unmatched_skus)[:10])}"
            )

        if duplicate:
            warnings.append(
                f"Este archivo ya fue subido el {duplicate['uploaded_at'][:10]} "
                f"({duplicate['filename']})"
            )

        # snapshot_date = today (file is a point-in-time export)
        snapshot_date = date.today()

        # Store in cache
        cache_data = {
            "rows": [r.model_dump(mode="json") for r in preview_rows],
            "snapshot_date": snapshot_date.isoformat(),
            "file_hash": file_hash,
            "filename": file.filename,
            "upload_type": "committed_orders",
        }
        preview_id = preview_cache_service.store_preview(cache_data)

        logger.info(
            "committed_orders_preview_created",
            preview_id=preview_id,
            row_count=len(preview_rows),
            matched=sum(1 for r in preview_rows if r.matched),
            unmatched=len(unmatched_skus),
        )

        return CommittedOrderPreview(
            preview_id=preview_id,
            row_count=len(preview_rows),
            snapshot_date=snapshot_date,
            warnings=warnings,
            rows=preview_rows,
            expires_in_minutes=30,
        )

    except ValueError as e:
        logger.error("committed_orders_parse_error", error=str(e))
        return JSONResponse(
            status_code=400,
            content={"error": {"code": "PARSE_ERROR", "message": str(e)}}
        )
    except (AppError,) as e:
        return _handle_error(e)
    except Exception as e:
        logger.error("committed_orders_preview_failed", error=str(e))
        return _handle_error(e)


@router.post("/confirm/{preview_id}", response_model=CommittedOrderResponse)
async def confirm_committed_orders(
    preview_id: str,
    request: Optional[CommittedOrderConfirmRequest] = None,
):
    """
    Save previously previewed committed orders data.

    Upserts to committed_orders table (on_conflict=product_id,snapshot_date).
    Only matched rows (with product_id) are saved.
    """
    try:
        cache_data = preview_cache_service.retrieve_preview(preview_id)
        if cache_data is None:
            raise HTTPException(status_code=404, detail="Preview expired")

        # Reconstruct rows from cache
        cached_rows = cache_data["rows"]
        snapshot_date = cache_data["snapshot_date"]

        # Apply modifications
        modifications = request.modifications if request else []
        deletions = request.deletions if request else []

        if modifications:
            mod_map = {m.sku: m for m in modifications}
            for row in cached_rows:
                sku = row.get("sku", "")
                if sku in mod_map:
                    mod = mod_map[sku]
                    if mod.quantity_committed is not None:
                        row["quantity_committed"] = round(mod.quantity_committed, 2)
            logger.info("committed_orders_modifications_applied", count=len(modifications))

        # Apply deletions (exclude rows by SKU)
        if deletions:
            deletion_set = set(deletions)
            cached_rows = [
                r for r in cached_rows
                if r.get("sku", "") not in deletion_set
            ]
            logger.info("committed_orders_deletions_applied", count=len(deletions))

        # Filter to only matched rows (have product_id)
        rows_to_upsert = [
            r for r in cached_rows
            if r.get("product_id") and r.get("matched", False)
        ]

        if not rows_to_upsert:
            return CommittedOrderResponse(
                success=True,
                records_upserted=0,
                snapshot_date=snapshot_date,
                message="No hay registros con productos reconocidos para guardar.",
            )

        # Upsert to committed_orders table
        db = get_supabase_client()
        upsert_data = [
            {
                "product_id": r["product_id"],
                "snapshot_date": snapshot_date,
                "quantity_committed": r["quantity_committed"],
                "warehouse_code": r.get("warehouse_code"),
                "order_reference": r.get("order_reference"),
            }
            for r in rows_to_upsert
        ]

        # Batch upsert in chunks
        chunk_size = 100
        for i in range(0, len(upsert_data), chunk_size):
            chunk = upsert_data[i:i + chunk_size]
            db.table("committed_orders").upsert(
                chunk, on_conflict="product_id,snapshot_date"
            ).execute()

        # Record upload history
        get_upload_history_service().record_upload(
            upload_type=cache_data.get("upload_type", "committed_orders"),
            file_hash=cache_data.get("file_hash", ""),
            filename=cache_data.get("filename", "unknown"),
            row_count=len(upsert_data),
        )

        # Delete preview from cache
        preview_cache_service.delete_preview(preview_id)

        logger.info(
            "committed_orders_confirm_complete",
            preview_id=preview_id,
            records_upserted=len(upsert_data),
        )

        return CommittedOrderResponse(
            success=True,
            records_upserted=len(upsert_data),
            snapshot_date=snapshot_date,
            message=f"Se guardaron {len(upsert_data)} registros de pedidos comprometidos.",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "committed_orders_confirm_failed",
            error=str(e),
            preview_id=preview_id,
        )
        return _handle_error(e)

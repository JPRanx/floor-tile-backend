"""
Unfulfilled demand (productos faltantes) API routes.

Upload Excel files with unfulfilled demand data (3-column format:
FECHA | PRODUCTOS | CANTIDADES) and upsert to unfulfilled_demand table.

Follows the preview-then-confirm pattern from inventory.py.
"""

from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import date
from io import BytesIO
import hashlib
import structlog
import pandas as pd

from models.unfulfilled_demand import (
    UnfulfilledDemandPreview,
    UnfulfilledDemandPreviewRow,
    UnfulfilledDemandConfirmRequest,
    UnfulfilledDemandResponse,
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


def _parse_unfulfilled_excel(content: bytes) -> list[dict]:
    """
    Parse unfulfilled demand Excel file.

    Expected format: 3 columns — FECHA | PRODUCTOS | CANTIDADES
    Returns list of dicts with keys: fecha, producto, cantidad.
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

    # Find header row: look for row containing "PRODUCTO" or "FECHA"
    header_row = 0
    for i in range(min(5, len(df))):
        row_text = " ".join(str(v).upper() for v in df.iloc[i] if pd.notna(v))
        if "PRODUCTO" in row_text or "FECHA" in row_text:
            header_row = i
            break

    # Re-read with header
    file_obj.seek(0)
    df = pd.read_excel(file_obj, header=header_row, engine=engine)
    df.columns = [str(c).upper().strip() for c in df.columns]

    # Map column names (handle variants)
    col_map = {}
    for col in df.columns:
        if "FECHA" in col:
            col_map["fecha"] = col
        elif "PRODUCTO" in col:
            col_map["producto"] = col
        elif "CANTIDAD" in col:
            col_map["cantidad"] = col

    missing = []
    if "fecha" not in col_map:
        missing.append("FECHA")
    if "producto" not in col_map:
        missing.append("PRODUCTOS")
    if "cantidad" not in col_map:
        missing.append("CANTIDADES")

    if missing:
        raise ValueError(
            f"Columnas faltantes: {', '.join(missing)}. "
            f"Se esperan: FECHA, PRODUCTOS, CANTIDADES"
        )

    rows = []
    for _, row in df.iterrows():
        fecha_val = row[col_map["fecha"]]
        producto_val = row[col_map["producto"]]
        cantidad_val = row[col_map["cantidad"]]

        # Skip empty rows
        if pd.isna(producto_val) or pd.isna(cantidad_val):
            continue

        # Parse date
        if pd.isna(fecha_val):
            continue
        if isinstance(fecha_val, str):
            try:
                fecha = pd.to_datetime(fecha_val).date()
            except Exception:
                continue
        else:
            try:
                fecha = pd.Timestamp(fecha_val).date()
            except Exception:
                continue

        # Parse quantity
        try:
            cantidad = float(cantidad_val)
        except (ValueError, TypeError):
            continue

        if cantidad <= 0:
            continue

        rows.append({
            "fecha": fecha,
            "producto": str(producto_val).strip(),
            "cantidad": round(cantidad, 2),
        })

    return rows


@router.post("/preview", response_model=UnfulfilledDemandPreview)
async def preview_unfulfilled_demand(file: UploadFile = File(...)):
    """
    Parse unfulfilled demand Excel and return preview.

    Expected format: FECHA | PRODUCTOS | CANTIDADES.
    Nothing is saved until /confirm is called.
    """
    try:
        content = await file.read()
        file_hash = hashlib.sha256(content).hexdigest()

        # Check for duplicate upload
        duplicate = get_upload_history_service().check_duplicate(
            "unfulfilled_demand", file_hash
        )

        # Parse Excel
        parsed_rows = _parse_unfulfilled_excel(content)

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

        # Match parsed rows against products
        warnings: list[str] = []
        preview_rows: list[UnfulfilledDemandPreviewRow] = []
        unmatched_skus: set[str] = set()

        # Group by product+date and sum quantities
        aggregated: dict[tuple[str, str], float] = {}
        for row in parsed_rows:
            key = (row["producto"], row["fecha"].isoformat())
            aggregated[key] = aggregated.get(key, 0.0) + row["cantidad"]

        for (producto, fecha_iso), cantidad in aggregated.items():
            normalized = _normalize_sku_name(producto)
            match = sku_map.get(normalized)

            if match:
                product_id, canonical_sku = match
                preview_rows.append(UnfulfilledDemandPreviewRow(
                    sku=canonical_sku,
                    product_id=product_id,
                    quantity_m2=round(cantidad, 2),
                    snapshot_date=date.fromisoformat(fecha_iso),
                    matched=True,
                ))
            else:
                unmatched_skus.add(producto)
                preview_rows.append(UnfulfilledDemandPreviewRow(
                    sku=producto,
                    product_id=None,
                    quantity_m2=round(cantidad, 2),
                    snapshot_date=date.fromisoformat(fecha_iso),
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

        # Determine snapshot_date from data (most common date)
        date_counts: dict[date, int] = {}
        for row in preview_rows:
            date_counts[row.snapshot_date] = date_counts.get(row.snapshot_date, 0) + 1
        snapshot_date = max(date_counts, key=date_counts.get) if date_counts else date.today()

        # Store in cache
        cache_data = {
            "rows": [r.model_dump(mode="json") for r in preview_rows],
            "snapshot_date": snapshot_date.isoformat(),
            "file_hash": file_hash,
            "filename": file.filename,
            "upload_type": "unfulfilled_demand",
        }
        preview_id = preview_cache_service.store_preview(cache_data)

        logger.info(
            "unfulfilled_demand_preview_created",
            preview_id=preview_id,
            row_count=len(preview_rows),
            matched=sum(1 for r in preview_rows if r.matched),
            unmatched=len(unmatched_skus),
        )

        return UnfulfilledDemandPreview(
            preview_id=preview_id,
            row_count=len(preview_rows),
            snapshot_date=snapshot_date,
            warnings=warnings,
            rows=preview_rows,
            expires_in_minutes=30,
        )

    except ValueError as e:
        logger.error("unfulfilled_demand_parse_error", error=str(e))
        return JSONResponse(
            status_code=400,
            content={"error": {"code": "PARSE_ERROR", "message": str(e)}}
        )
    except (AppError,) as e:
        return _handle_error(e)
    except Exception as e:
        logger.error("unfulfilled_demand_preview_failed", error=str(e))
        return _handle_error(e)


@router.post("/confirm/{preview_id}", response_model=UnfulfilledDemandResponse)
async def confirm_unfulfilled_demand(
    preview_id: str,
    request: Optional[UnfulfilledDemandConfirmRequest] = None,
):
    """
    Save previously previewed unfulfilled demand data.

    Upserts to unfulfilled_demand table (on_conflict=product_id,snapshot_date).
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
                    if mod.quantity_m2 is not None:
                        row["quantity_m2"] = round(mod.quantity_m2, 2)
            logger.info("unfulfilled_demand_modifications_applied", count=len(modifications))

        # Apply deletions (exclude rows by SKU)
        if deletions:
            deletion_set = set(deletions)
            cached_rows = [
                r for r in cached_rows
                if r.get("sku", "") not in deletion_set
            ]
            logger.info("unfulfilled_demand_deletions_applied", count=len(deletions))

        # Filter to only matched rows (have product_id)
        rows_to_upsert = [
            r for r in cached_rows
            if r.get("product_id") and r.get("matched", False)
        ]

        if not rows_to_upsert:
            return UnfulfilledDemandResponse(
                success=True,
                records_upserted=0,
                snapshot_date=snapshot_date,
                message="No hay registros con productos reconocidos para guardar.",
            )

        # Upsert to unfulfilled_demand table
        db = get_supabase_client()
        upsert_data = [
            {
                "product_id": r["product_id"],
                "snapshot_date": r["snapshot_date"],
                "quantity_m2": r["quantity_m2"],
            }
            for r in rows_to_upsert
        ]

        # Batch upsert in chunks
        chunk_size = 100
        for i in range(0, len(upsert_data), chunk_size):
            chunk = upsert_data[i:i + chunk_size]
            db.table("unfulfilled_demand").upsert(
                chunk, on_conflict="product_id,snapshot_date"
            ).execute()

        # Record upload history
        get_upload_history_service().record_upload(
            upload_type=cache_data.get("upload_type", "unfulfilled_demand"),
            file_hash=cache_data.get("file_hash", ""),
            filename=cache_data.get("filename", "unknown"),
            row_count=len(upsert_data),
        )

        # Delete preview from cache
        preview_cache_service.delete_preview(preview_id)

        logger.info(
            "unfulfilled_demand_confirm_complete",
            preview_id=preview_id,
            records_upserted=len(upsert_data),
        )

        return UnfulfilledDemandResponse(
            success=True,
            records_upserted=len(upsert_data),
            snapshot_date=snapshot_date,
            message=f"Se guardaron {len(upsert_data)} registros de demanda insatisfecha.",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "unfulfilled_demand_confirm_failed",
            error=str(e),
            preview_id=preview_id,
        )
        return _handle_error(e)

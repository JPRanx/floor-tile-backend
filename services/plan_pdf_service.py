"""PDF renderer for order plans.

Layout mirrors the reference PDF: KPIs, boat table, velocity ranking,
per-boat cards, skipped list, capacity check, narrative, manual edits.
"""

from __future__ import annotations

from datetime import date
from io import BytesIO
from typing import Any

import structlog
from fpdf import FPDF


logger = structlog.get_logger(__name__)

M2_PER_PALLET = 134.4
PALLETS_PER_CONTAINER = 13


# Strip characters outside latin-1 (fpdf's default helvetica has no unicode)
_LATIN1_REPLACEMENTS = {
    "—": "-", "–": "-", "\u2013": "-", "\u2014": "-",
    "\u2018": "'", "\u2019": "'", "\u201c": '"', "\u201d": '"',
    "\u2026": "...",
}


def _safe(s: str) -> str:
    for k, v in _LATIN1_REPLACEMENTS.items():
        s = s.replace(k, v)
    # Drop any remaining non-latin1 bytes
    return s.encode("latin-1", "replace").decode("latin-1")


class _PDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(120, 120, 120)
        self.cell(0, 6, _safe("CM Tarragona - Confidencial"), align="R",
                  new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Pagina {self.page_no()}/{{nb}}", align="C")


def _section_title(pdf: _PDF, title: str):
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(30, 30, 30)
    pdf.cell(0, 9, _safe(title), new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(60, 60, 60)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
    pdf.ln(3)


def _body(pdf: _PDF, text: str):
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(40, 40, 40)
    pdf.multi_cell(0, 5, _safe(text))
    pdf.ln(2)


def _thead(pdf: _PDF, cols: list[str], widths: list[float]):
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(45, 55, 72)
    pdf.set_text_color(255, 255, 255)
    for i, c in enumerate(cols):
        pdf.cell(widths[i], 7, _safe(c), border=1, fill=True, align="C")
    pdf.ln()


def _trow(pdf: _PDF, cells: list[str], widths: list[float],
          aligns: list[str], highlight: bool = False):
    pdf.set_font("Helvetica", "", 8)
    if highlight:
        pdf.set_fill_color(254, 243, 199)
        pdf.set_text_color(120, 53, 15)
    else:
        pdf.set_fill_color(248, 250, 252)
        pdf.set_text_color(40, 40, 40)
    for i, c in enumerate(cells):
        pdf.cell(widths[i], 6, _safe(str(c)), border=1,
                 fill=highlight, align=aligns[i])
    pdf.ln()


def _ttotal(pdf: _PDF, cells: list[str], widths: list[float],
            aligns: list[str]):
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(45, 55, 72)
    pdf.set_text_color(255, 255, 255)
    for i, c in enumerate(cells):
        pdf.cell(widths[i], 7, _safe(str(c)), border=1, fill=True, align=aligns[i])
    pdf.ln()


def _kpi(pdf: _PDF, label: str, value: str, x: float, y: float,
         w: float = 42, h: float = 18):
    pdf.set_xy(x, y)
    pdf.set_fill_color(45, 55, 72)
    pdf.set_draw_color(45, 55, 72)
    pdf.rect(x, y, w, h, "F")
    pdf.set_xy(x, y + 2)
    pdf.set_font("Helvetica", "", 7)
    pdf.set_text_color(180, 200, 220)
    pdf.cell(w, 4, _safe(label), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_xy(x, y + 7)
    pdf.set_font("Helvetica", "B", 12)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(w, 8, _safe(value), align="C")


def _fmt_date(iso_str: str) -> str:
    """YYYY-MM-DD → DD-Mon in Spanish."""
    try:
        d = date.fromisoformat(iso_str[:10])
    except Exception:
        return iso_str
    months = {1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
              7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"}
    return f"{d.day:02d}-{months[d.month]}"


def _build_edit_deltas(adjusted: list[Any], original: Any) -> list[str]:
    """Compute human-readable deltas between original plan and adjusted."""
    orig_map: dict[tuple[str, str], int] = {}
    for b in original.boats:
        for ln in b.lines:
            orig_map[(b.boat_id, ln.product_id)] = ln.pallets

    adj_map: dict[tuple[str, str], tuple[int, str, str]] = {}
    for b in adjusted:
        for ln in b.lines:
            adj_map[(b.boat_id, ln.product_id)] = (ln.pallets, b.vessel_name, ln.sku)

    deltas: list[str] = []

    # Changes + additions
    for key, (new_pallets, vessel, sku) in adj_map.items():
        orig_pallets = orig_map.get(key, 0)
        if new_pallets != orig_pallets:
            if orig_pallets == 0:
                deltas.append(f"{sku} anadido a {vessel}: {new_pallets} pallets")
            else:
                sign = "+" if new_pallets > orig_pallets else ""
                deltas.append(
                    f"{sku} en {vessel}: {orig_pallets} -> {new_pallets} pallets "
                    f"({sign}{new_pallets - orig_pallets})"
                )

    # Removals (original had, adjusted doesn't)
    for key, orig_pallets in orig_map.items():
        if key not in adj_map and orig_pallets > 0:
            # Find vessel name + sku from original
            for b in original.boats:
                if b.boat_id == key[0]:
                    for ln in b.lines:
                        if ln.product_id == key[1]:
                            deltas.append(f"{ln.sku} removido de {b.vessel_name}")
                            break
                    break

    return deltas


def render_plan_pdf(adjusted: list[Any], original: Any, narrative: str) -> bytes:
    """Render the plan as a PDF. Returns raw bytes.

    Args:
        adjusted: List of AdjustedBoat (Pydantic) with Ashley's final numbers.
        original: GenerateResponse (Pydantic) — original system proposal.
        narrative: Frozen AI narrative from /generate.
    """
    pdf = _PDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Totals from adjusted plan
    total_pallets = sum(
        ln.pallets for b in adjusted for ln in b.lines
    )
    total_containers = round(total_pallets / PALLETS_PER_CONTAINER, 1)
    total_m2 = total_pallets * M2_PER_PALLET

    # Title
    pdf.set_font("Helvetica", "B", 18)
    pdf.set_text_color(30, 30, 30)
    pdf.cell(0, 11, _safe("Plan de Pedidos por Velocidad"), new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 6, _safe(
        f"{len(adjusted)} buques - Generado {date.today().isoformat()}"
    ), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)

    # KPIs
    y = pdf.get_y()
    _kpi(pdf, "SIESA Total", f"{original.total_siesa_pallets} pallets", 10, y)
    _kpi(pdf, "Plan Total", f"{total_pallets} pallets", 56, y)
    _kpi(pdf, "Contenedores", f"{total_containers}", 102, y)
    _kpi(pdf, "Volumen", f"{total_m2:,.0f} m2", 148, y)
    pdf.set_xy(10, y + 22)
    pdf.ln(4)

    # Narrative
    _section_title(pdf, "Resumen Ejecutivo")
    _body(pdf, narrative)

    # Boats overview
    _section_title(pdf, "Buques Seleccionados")
    bcols = ["Buque", "Salida", "Llegada", "Contenedores", "Pallets", "m2"]
    bwidths = [44, 24, 24, 32, 22, 30]
    baligns = ["L", "C", "C", "C", "R", "R"]
    _thead(pdf, bcols, bwidths)
    for b in adjusted:
        boat_pallets = sum(ln.pallets for ln in b.lines)
        boat_containers = round(boat_pallets / PALLETS_PER_CONTAINER, 1)
        boat_m2 = boat_pallets * M2_PER_PALLET
        _trow(pdf, [
            b.vessel_name,
            _fmt_date(b.departure_date),
            _fmt_date(b.arrival_date),
            f"{boat_containers} / {b.max_containers}",
            str(boat_pallets),
            f"{boat_m2:,.0f}",
        ], bwidths, baligns)
    _ttotal(pdf, ["TOTAL", "", "", f"{total_containers}", str(total_pallets),
                  f"{total_m2:,.0f}"], bwidths, baligns)
    pdf.ln(4)

    # Per-boat cards
    # Build lookup from original for reasoning context per line
    orig_line_map: dict[tuple[str, str], Any] = {}
    for b in original.boats:
        for ln in b.lines:
            orig_line_map[(b.boat_id, ln.product_id)] = ln

    for b in adjusted:
        pdf.add_page()
        boat_pallets = sum(ln.pallets for ln in b.lines)
        boat_containers = round(boat_pallets / PALLETS_PER_CONTAINER, 1)
        boat_m2 = boat_pallets * M2_PER_PALLET
        _section_title(pdf, f"{b.vessel_name} ({_fmt_date(b.departure_date)})")
        _body(pdf,
              f"Carga: {boat_pallets} pallets / {boat_containers} contenedores "
              f"/ {boat_m2:,.0f} m2")

        cols = ["SKU", "Pallets", "Cont.", "m2", "Razon"]
        widths = [52, 18, 16, 26, 65]
        aligns = ["L", "R", "R", "R", "L"]
        _thead(pdf, cols, widths)
        for ln in b.lines:
            orig = orig_line_map.get((b.boat_id, ln.product_id))
            note = orig.note_es if orig else "-"
            line_m2 = ln.pallets * M2_PER_PALLET
            line_cont = round(ln.pallets / PALLETS_PER_CONTAINER, 1)
            _trow(pdf, [
                ln.sku,
                str(ln.pallets),
                f"{line_cont}",
                f"{line_m2:,.0f}",
                note,
            ], widths, aligns, highlight=(orig is not None and orig.is_urgent))
        _ttotal(pdf, ["TOTAL", str(boat_pallets), f"{boat_containers}",
                      f"{boat_m2:,.0f}", ""], widths, aligns)
        pdf.ln(4)

    # Skipped
    if original.skipped:
        pdf.add_page()
        _section_title(pdf, "No Cargar (Quedan en SIESA)")
        _body(pdf, "Productos sin demanda confirmada en los ultimos 90 dias.")
        s_cols = ["SKU", "Pallets", "m2", "Razon"]
        s_widths = [50, 22, 28, 70]
        s_aligns = ["L", "R", "R", "L"]
        _thead(pdf, s_cols, s_widths)
        for s in original.skipped:
            _trow(pdf, [s.sku, f"{s.siesa_pallets:.1f}",
                        f"{s.siesa_m2:,.0f}", s.reason_es],
                  s_widths, s_aligns)
        pdf.ln(4)

    # Capacity check
    cap = original.warehouse_capacity
    _section_title(pdf, "Verificacion de Capacidad de Bodega")
    cap_cols = ["Concepto", "Pallets"]
    cap_widths = [140, 40]
    cap_aligns = ["L", "R"]
    _thead(pdf, cap_cols, cap_widths)
    _trow(pdf, ["Inventario actual", str(cap.current_pallets)], cap_widths, cap_aligns)
    _trow(pdf, ["Entrante de buques ya cargados", f"+{cap.incoming_pallets}"],
          cap_widths, cap_aligns)
    _trow(pdf, ["Plan de buques nuevos", f"+{cap.plan_pallets}"], cap_widths, cap_aligns)
    _trow(pdf, ["Salida estimada (3 semanas)", f"-{cap.outflow_pallets}"],
          cap_widths, cap_aligns)
    _ttotal(pdf, ["PICO ESTIMADO", str(cap.peak_pallets)], cap_widths, cap_aligns)
    _trow(pdf, ["Capacidad maxima", str(cap.max_pallets)], cap_widths, cap_aligns)
    _trow(pdf, ["Utilizacion pico", f"{cap.utilization_pct}%"], cap_widths, cap_aligns)
    pdf.ln(3)
    status = "Dentro de capacidad - sin riesgo de demurrage" if cap.is_safe \
        else "ALERTA - plan excede el buffer de bodega"
    _body(pdf, f"Estado: {status}")

    # Manual edits
    deltas = _build_edit_deltas(adjusted, original)
    if deltas:
        _section_title(pdf, "Ajustes Manuales de Ashley")
        _body(pdf, "La IA genero el plan inicial. Ashley realizo los siguientes cambios:")
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(40, 40, 40)
        for d in deltas:
            pdf.cell(5, 5, "")
            pdf.cell(0, 5, _safe(f"- {d}"), new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

    buf = BytesIO()
    pdf.output(buf)
    return buf.getvalue()

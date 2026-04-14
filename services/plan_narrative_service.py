"""Claude narrative generation for order plans.

Produces a short executive summary in Spanish based on the deterministic
plan numbers. LLM never touches numbers — only writes prose around them.
Falls back to a templated default if the API is unavailable.
"""

from __future__ import annotations

import structlog

from config import settings
from services.order_plan_service import PlanResult


logger = structlog.get_logger(__name__)

MODEL = "claude-haiku-4-5-20251001"
MAX_TOKENS = 500


_FALLBACK_TEMPLATE = (
    "Este plan de pedidos prioriza los productos con mayor velocidad de venta "
    "disponibles en SIESA. La asignacion se distribuye en cascada entre los "
    "buques seleccionados, llenando primero los productos de mayor rotacion. "
    "Revisar la tabla por buque para confirmar cantidades antes de exportar."
)


def generate_narrative(result: PlanResult) -> str:
    """Generate a Spanish executive summary for the plan.

    Deterministic numbers get passed in; Claude writes the prose.
    Returns fallback text if no API key or on error.
    """
    if not settings.anthropic_api_key:
        logger.warning("plan_narrative_no_key")
        return _FALLBACK_TEMPLATE

    try:
        import anthropic
    except ImportError:
        logger.warning("anthropic_sdk_missing")
        return _FALLBACK_TEMPLATE

    # Build a compact structured summary for Claude to narrate
    top_lines: list[str] = []
    for boat in result.boats:
        line_summary = ", ".join(
            f"{ln.sku} ({ln.pallets}p)"
            for ln in boat.lines[:5]
        )
        more = f" +{len(boat.lines) - 5} mas" if len(boat.lines) > 5 else ""
        top_lines.append(
            f"- {boat.vessel_name} ({boat.departure_date}): "
            f"{boat.total_pallets} pallets / {boat.containers_used} contenedores"
            f" | Top: {line_summary}{more}"
        )

    urgent_skus = [
        r.sku for r in result.velocity_ranking[:5] if r.is_urgent
    ]

    prompt = f"""Eres un asistente logistico para una importadora de baldosas en Centro America.
Resume este plan de pedidos en 2 parrafos cortos en espanol, tono profesional y directo.
NO inventes numeros — solo comenta sobre los datos que te doy.

Plan generado:
- Buques: {len(result.boats)}
- Pallets totales: {result.plan_total_pallets}
- SIESA total disponible: {result.total_siesa_pallets} pallets
- SIESA asignado: {result.plan_total_pallets} de {result.total_siesa_pallets} pallets ({round(result.plan_total_pallets / result.total_siesa_pallets * 100) if result.total_siesa_pallets else 0}%)
- Productos omitidos: {len(result.skipped)}
- Utilizacion de bodega pico: {result.warehouse_capacity.utilization_pct}%
- Seguro de capacidad: {'si' if result.warehouse_capacity.is_safe else 'NO - ALERTA'}
- SKUs urgentes (<4 sem cobertura): {', '.join(urgent_skus) if urgent_skus else 'ninguno'}

Distribucion:
{chr(10).join(top_lines)}

Escribe 2 parrafos cortos:
1. Un resumen de que logra este plan (cuanto SIESA vacia, cuantos productos, principal estrategia)
2. Puntos de atencion (productos urgentes, capacidad de bodega, productos omitidos si hay)

Sin viñetas, sin encabezados. Solo prosa fluida. Maximo 150 palabras en total."""

    try:
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        message = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        text_blocks = [
            block.text for block in message.content if block.type == "text"
        ]
        text = "\n\n".join(text_blocks).strip()
        if not text:
            return _FALLBACK_TEMPLATE
        logger.info(
            "plan_narrative_generated",
            input_tokens=message.usage.input_tokens,
            output_tokens=message.usage.output_tokens,
        )
        return text
    except Exception as exc:
        logger.warning("plan_narrative_failed", error=str(exc))
        return _FALLBACK_TEMPLATE

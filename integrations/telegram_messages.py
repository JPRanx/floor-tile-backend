"""
Centralized Telegram message templates with i18n support.

Usage:
    from integrations.telegram_messages import get_message

    message = get_message("email_processed",
        from_addr="sender@example.com",
        doc_type="BL",
        booking="ABC123",
        action="Updated",
        confidence=95
    )
"""

import os

# Language setting - defaults to Spanish
LANG = os.getenv("TELEGRAM_LANGUAGE", "es")

MESSAGES = {
    "en": {
        # Email ingestion messages
        "email_processed": """✅ *Email processed*

From: `{from_addr}`
Document: {doc_type}
Booking: `{booking}`
Action: {action}
Confidence: {confidence:.0%}""",

        "email_needs_review": """⚠️ *Email needs review*

From: `{from_addr}`
Subject: {subject}

Reason: {reason}
{details}
{pending_link}
Please review manually in the system.""",

        "email_error": """❌ *Email processing failed*

From: `{from_addr}`
Subject: {subject}

Error: {error}

Please check the email manually.""",

        # Email needs review details
        "email_review_details": """
Document type: {doc_type}
Booking: `{booking}`
Containers: {containers}
Confidence: {confidence:.0%}""",

        "email_pending_link": """

🔗 [Review in dashboard](/pending-documents/{pending_id})""",

        # Shipment status changes
        "shipment_departed": """🚢 Shipment {shp_number} has departed

Vessel: {vessel}
ETA: {eta}""",

        "shipment_at_port": """⚓ Shipment {shp_number} arrived at port

Vessel: {vessel}
Containers: {containers}""",

        "shipment_delivered": """✅ Shipment {shp_number} delivered

Vessel: {vessel}
Products received at warehouse""",

        # Ingest messages
        "new_shipment_created": """📦 New shipment created: {shp_number}

Booking: {booking}
Vessel: {vessel}""",

        "ingest_confirmed": """✅ Document ingested

Shipment: {shp_number}
Type: {doc_type}
Status: {status}""",

        # Alert messages
        "stockout_warning": """⚠️ *Stockout warning: {sku}*

Low stock alert for {sku}

Stockout in {days} days
Current stock: {stock} m²
Daily usage: {daily_usage} m²/day""",

        "booking_deadline": """⏰ *Booking deadline approaching*

Boat: {vessel}
Deadline: {deadline}
Days remaining: {days}

Book containers now to avoid missing this shipment.""",

        # Titles for alerts
        "title_stockout": "Stockout warning: {sku}",
        "title_booking_deadline": "Booking deadline: {vessel}",
        "title_shipment_departed": "Shipment departed: {shp_number}",
        "title_shipment_at_port": "Shipment at port: {shp_number}",
        "title_shipment_delivered": "Shipment delivered: {shp_number}",
        "title_new_shipment": "New shipment: {shp_number}",
    },

    "es": {
        # Email ingestion messages
        "email_processed": """✅ *Correo procesado*

De: `{from_addr}`
Documento: {doc_type}
Reserva: `{booking}`
Acción: {action}
Confianza: {confidence:.0%}""",

        "email_needs_review": """⚠️ *Correo requiere revisión*

De: `{from_addr}`
Asunto: {subject}

Razón: {reason}
{details}
{pending_link}
Por favor revisar manualmente en el sistema.""",

        "email_error": """❌ *Error procesando correo*

De: `{from_addr}`
Asunto: {subject}

Error: {error}

Por favor revisar el correo manualmente.""",

        # Email needs review details
        "email_review_details": """
Tipo de documento: {doc_type}
Reserva: `{booking}`
Contenedores: {containers}
Confianza: {confidence:.0%}""",

        "email_pending_link": """

🔗 [Revisar en dashboard](/pending-documents/{pending_id})""",

        # Shipment status changes
        "shipment_departed": """🚢 Embarque {shp_number} ha zarpado

Buque: {vessel}
ETA: {eta}""",

        "shipment_at_port": """⚓ Embarque {shp_number} llegó al puerto

Buque: {vessel}
Contenedores: {containers}""",

        "shipment_delivered": """✅ Embarque {shp_number} entregado

Buque: {vessel}
Productos recibidos en almacén""",

        # Ingest messages
        "new_shipment_created": """📦 Nuevo embarque creado: {shp_number}

Booking: {booking}
Buque: {vessel}""",

        "ingest_confirmed": """✅ Documento ingresado

Embarque: {shp_number}
Tipo: {doc_type}
Estado: {status}""",

        # Alert messages
        "stockout_warning": """⚠️ *Alerta de desabasto: {sku}*

Alerta de stock bajo para {sku}

Desabasto en {days} días
Stock actual: {stock} m²
Consumo diario: {daily_usage} m²/día""",

        "booking_deadline": """⏰ *Fecha límite de reserva próxima*

Barco: {vessel}
Fecha límite: {deadline}
Días restantes: {days}

Reserve contenedores ahora para no perder este embarque.""",

        # Titles for alerts
        "title_stockout": "Alerta de desabasto: {sku}",
        "title_booking_deadline": "Fecha límite de reserva: {vessel}",
        "title_shipment_departed": "Embarque zarpó: {shp_number}",
        "title_shipment_at_port": "Embarque en puerto: {shp_number}",
        "title_shipment_delivered": "Embarque entregado: {shp_number}",
        "title_new_shipment": "Nuevo embarque: {shp_number}",
    },
}


def get_message(key: str, **kwargs) -> str:
    """
    Get translated message template and format with kwargs.

    Args:
        key: Message template key
        **kwargs: Format arguments for the template

    Returns:
        Formatted message string in the configured language
    """
    lang_messages = MESSAGES.get(LANG, MESSAGES["es"])
    template = lang_messages.get(key, MESSAGES["en"].get(key, key))
    try:
        return template.format(**kwargs)
    except KeyError as e:
        # Return template as-is if formatting fails
        return template


def get_lang() -> str:
    """Get current language setting."""
    return LANG

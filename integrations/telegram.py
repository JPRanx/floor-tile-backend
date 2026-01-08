"""
Telegram bot integration for sending alerts.

Sends formatted alert messages to a Telegram channel/chat.
"""

import os
from typing import Optional
import requests
import structlog

from models.alert import AlertResponse, AlertType, AlertSeverity

logger = structlog.get_logger(__name__)


# Emoji mappings for alert types and severities
SEVERITY_EMOJIS = {
    "CRITICAL": "🚨",
    "WARNING": "⚠️",
    "INFO": "ℹ️",
}

TYPE_EMOJIS = {
    "STOCKOUT_WARNING": "📉",
    "LOW_STOCK": "📦",
    "ORDER_OPPORTUNITY": "🛒",
    "SHIPMENT_DEPARTED": "🚢",
    "SHIPMENT_ARRIVED": "✅",
    "FREE_DAYS_EXPIRING": "⏰",
    "SHIPMENT_DELAYED": "⏱️",
    "CONTAINER_READY": "📦",
    "OVER_STOCKED": "📈",
}


class TelegramError(Exception):
    """Telegram API error."""
    pass


def get_telegram_config() -> tuple[Optional[str], Optional[str]]:
    """
    Get Telegram configuration from environment.

    Returns:
        tuple: (bot_token, chat_id)
    """
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        logger.warning(
            "telegram_not_configured",
            has_token=bool(bot_token),
            has_chat_id=bool(chat_id)
        )

    return bot_token, chat_id


def format_alert_message(alert: AlertResponse) -> str:
    """
    Format alert as Telegram message with emojis and formatting.

    Args:
        alert: Alert to format

    Returns:
        Formatted message string
    """
    severity_emoji = SEVERITY_EMOJIS.get(alert.severity, "•")
    type_emoji = TYPE_EMOJIS.get(alert.type, "•")

    # Build message
    lines = [
        f"{severity_emoji} {alert.severity.upper()}",
        "",
        f"{type_emoji} *{alert.title}*",
        "",
        alert.message,
    ]

    # Add product info if available
    if alert.product_sku:
        lines.append("")
        lines.append(f"📦 Product: `{alert.product_sku}`")

    # Add shipment info if available
    if alert.shipment_booking_number:
        lines.append("")
        lines.append(f"🚢 Booking: `{alert.shipment_booking_number}`")

    # Add timestamp
    timestamp = alert.created_at.strftime("%Y-%m-%d %H:%M UTC")
    lines.append("")
    lines.append(f"🕐 {timestamp}")

    return "\n".join(lines)


def send_message(message: str, parse_mode: str = "Markdown") -> bool:
    """
    Send message to Telegram.

    Args:
        message: Message text to send
        parse_mode: Telegram parse mode (Markdown or HTML)

    Returns:
        True if sent successfully

    Raises:
        TelegramError: If send fails
    """
    bot_token, chat_id = get_telegram_config()

    if not bot_token or not chat_id:
        logger.warning("telegram_not_configured_skipping_send")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }

    try:
        logger.info("sending_telegram_message", chat_id=chat_id)

        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()

        result = response.json()

        if not result.get("ok"):
            error_msg = result.get("description", "Unknown error")
            logger.error("telegram_api_error", error=error_msg)
            raise TelegramError(f"Telegram API error: {error_msg}")

        logger.info("telegram_message_sent", message_id=result.get("result", {}).get("message_id"))
        return True

    except requests.exceptions.RequestException as e:
        logger.error("telegram_request_failed", error=str(e))
        raise TelegramError(f"Failed to send Telegram message: {str(e)}")


def send_alert_to_telegram(alert: AlertResponse) -> bool:
    """
    Send alert to Telegram with formatted message.

    Args:
        alert: Alert to send

    Returns:
        True if sent successfully

    Raises:
        TelegramError: If send fails
    """
    message = format_alert_message(alert)
    return send_message(message)


def test_connection() -> dict:
    """
    Test Telegram bot connection.

    Returns:
        dict with bot info and connection status

    Raises:
        TelegramError: If connection fails
    """
    bot_token, chat_id = get_telegram_config()

    if not bot_token:
        return {
            "configured": False,
            "error": "TELEGRAM_BOT_TOKEN not set"
        }

    if not chat_id:
        return {
            "configured": False,
            "error": "TELEGRAM_CHAT_ID not set"
        }

    # Get bot info
    url = f"https://api.telegram.org/bot{bot_token}/getMe"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        result = response.json()

        if not result.get("ok"):
            error_msg = result.get("description", "Unknown error")
            raise TelegramError(f"Telegram API error: {error_msg}")

        bot_info = result.get("result", {})

        # Send test message
        test_msg = "✅ *Telegram Integration Test*\n\nConnection successful! Your floor-tile-saas alerts are now configured."
        send_message(test_msg)

        return {
            "configured": True,
            "bot_username": bot_info.get("username"),
            "bot_name": bot_info.get("first_name"),
            "chat_id": chat_id,
            "test_message_sent": True,
        }

    except requests.exceptions.RequestException as e:
        logger.error("telegram_connection_test_failed", error=str(e))
        raise TelegramError(f"Failed to test Telegram connection: {str(e)}")
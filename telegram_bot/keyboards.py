"""Inline keyboard UI for the pytdbot (TDLib) client.

Callback data is stored as plain UTF-8 bytes in the format ``action:value``
(or just ``action`` when there is no value).  The bytes representation is
used directly as the ``data`` field of ``InlineKeyboardButtonTypeCallback``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pytdbot import types

if TYPE_CHECKING:
    from telegram_bot.settings import EncodeProfile


# ---------------------------------------------------------------------------
# Action identifiers
# ---------------------------------------------------------------------------

ACTION_PROFILE = "profile"
ACTION_DOCUMENT = "doc"
ACTION_CANCEL = "cancel"
ACTION_REFRESH = "refresh"
ACTION_SETTINGS = "settings"
ACTION_CLOSE = "close"


# ---------------------------------------------------------------------------
# Payload encode / decode
# ---------------------------------------------------------------------------

def _encode_payload(action: str, value: str = "") -> bytes:
    """Encode action + optional value into callback bytes."""
    raw = f"{action}:{value}" if value else action
    return raw.encode("utf-8")


def parse_callback_payload(payload: object) -> tuple[str, str]:
    """Decode a ``CallbackQueryPayload`` into ``(action, value)``.

    Handles:
    * ``types.CallbackQueryPayloadData`` — the normal case
    * ``bytes`` / ``str`` — defensive fallback
    * Anything else → ``("unknown", "")``
    """
    raw = ""

    if isinstance(payload, types.CallbackQueryPayloadData):
        data = payload.data
        if isinstance(data, (bytes, bytearray)):
            raw = data.decode("utf-8", errors="replace")
        elif isinstance(data, str):
            raw = data

    elif isinstance(payload, (bytes, bytearray)):
        raw = payload.decode("utf-8", errors="replace")

    elif isinstance(payload, str):
        raw = payload

    if not raw:
        return "unknown", ""

    if ":" in raw:
        action, _, value = raw.partition(":")
        return action.strip(), value.strip()

    return raw.strip(), ""


# ---------------------------------------------------------------------------
# Button helpers
# ---------------------------------------------------------------------------

def _cb_btn(text: str, action: str, value: str = "") -> types.InlineKeyboardButton:
    """Create an inline keyboard button with a callback payload."""
    return types.InlineKeyboardButton(
        text=text,
        type=types.InlineKeyboardButtonTypeCallback(
            data=_encode_payload(action, value)
        ),
    )


# ---------------------------------------------------------------------------
# Keyboard builders
# ---------------------------------------------------------------------------

def build_settings_keyboard(
    current_profile: str,
    send_as_document: bool,
    profiles: dict[str, "EncodeProfile"],
) -> types.ReplyMarkupInlineKeyboard:
    """Inline keyboard for profile selection and document-mode toggle."""
    buttons: list[list[types.InlineKeyboardButton]] = []

    # Profile rows — two per row
    profile_keys = list(profiles.keys())
    for i in range(0, len(profile_keys), 2):
        row: list[types.InlineKeyboardButton] = []
        for key in profile_keys[i : i + 2]:
            label = f"{'✓ ' if key == current_profile else ''}{profiles[key].name}"
            row.append(_cb_btn(label, ACTION_PROFILE, key))
        buttons.append(row)

    # Document toggle
    doc_label = f"📄 Send as Document: {'✓ ON' if send_as_document else '○ OFF'}"
    doc_value = "off" if send_as_document else "on"
    buttons.append([_cb_btn(doc_label, ACTION_DOCUMENT, doc_value)])

    # Close
    buttons.append([_cb_btn("❌ Close", ACTION_CLOSE)])

    return types.ReplyMarkupInlineKeyboard(buttons)


def build_queue_keyboard(job_ids: list[int]) -> types.ReplyMarkupInlineKeyboard:
    """Inline keyboard for queue status — refresh + per-job cancel buttons."""
    buttons: list[list[types.InlineKeyboardButton]] = []

    buttons.append([_cb_btn("🔄 Refresh", ACTION_REFRESH)])

    if job_ids:
        cancel_row: list[types.InlineKeyboardButton] = []
        for job_id in job_ids[:3]:
            cancel_row.append(_cb_btn(f"❌ Cancel #{job_id}", ACTION_CANCEL, str(job_id)))
            if len(cancel_row) == 3:
                buttons.append(cancel_row)
                cancel_row = []
        if cancel_row:
            buttons.append(cancel_row)

    buttons.append([_cb_btn("❌ Close", ACTION_CLOSE)])
    return types.ReplyMarkupInlineKeyboard(buttons)


def build_main_menu_keyboard() -> types.ReplyMarkupInlineKeyboard:
    """Inline keyboard for the /start welcome message."""
    return types.ReplyMarkupInlineKeyboard(
        [
            [_cb_btn("⚙️ Settings", ACTION_SETTINGS)],
            [_cb_btn("📋 Queue Status", ACTION_REFRESH)],
            [_cb_btn("❌ Close", ACTION_CLOSE)],
        ]
    )

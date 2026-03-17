import logging
from pathlib import Path
from typing import Union

import requests

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}"
TOKEN_FILE = "api_keys/telegram_bot_token.txt"


def load_token(token: str = None) -> str:
    """Return the provided token, or read it from the token file."""
    if token:
        return token
    path = Path(TOKEN_FILE)
    if not path.exists():
        raise FileNotFoundError(
            f"[load_token] Telegram token not provided and file not found: {TOKEN_FILE}"
        )
    return path.read_text().strip()


def _call_api(token: str, method: str, data: dict = None, files: dict = None) -> dict:
    """Make a request to the Telegram Bot API."""
    url = f"{TELEGRAM_API_BASE.format(token=token)}/{method}"
    if files:
        response = requests.post(url, data=data, files=files)
    else:
        response = requests.post(url, json=data)
    result = response.json()
    if not result.get("ok"):
        desc = result.get("description", "unknown error")
        logger.error(f"[_call_api] Telegram API error: {desc}")
        raise RuntimeError(f"Telegram API error: {desc}")
    return result


def send_message(
    chat_id: str,
    message: str,
    token: str = None,
    parse_mode: str = "HTML",
) -> dict:
    """
    Send a text message to a Telegram recipient.

    Args:
        chat_id: Target chat ID (user, group, or channel).
        message: Text content to send.
        token: Bot token. If None, reads from api_keys/telegram_bot_token.txt.
        parse_mode: Message formatting ("HTML" or "MarkdownV2").
    """
    token = load_token(token)
    payload = {"chat_id": chat_id, "text": message, "parse_mode": parse_mode}
    logger.info(f"[send_message] Sending message to chat_id={chat_id}")
    result = _call_api(token, "sendMessage", data=payload)
    logger.info(f"[send_message] Message sent successfully")
    return result


def send_image(
    chat_id: str,
    image_path: str,
    caption: str = None,
    token: str = None,
    parse_mode: str = "HTML",
) -> dict:
    """
    Send an image to a Telegram recipient.

    Args:
        chat_id: Target chat ID.
        image_path: Path to the image file to send.
        caption: Optional caption for the image.
        token: Bot token. If None, reads from api_keys/telegram_bot_token.txt.
        parse_mode: Message formatting for the caption.
    """
    token = load_token(token)
    path = Path(image_path)
    if not path.exists():
        raise FileNotFoundError(f"[send_image] Image not found: {image_path}")

    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
        data["parse_mode"] = parse_mode

    logger.info(f"[send_image] Sending image to chat_id={chat_id}: {image_path}")
    with open(path, "rb") as f:
        result = _call_api(token, "sendPhoto", data=data, files={"photo": f})
    logger.info(f"[send_image] Image sent successfully")
    return result


def send_to_multiple(
    chat_ids: list[str],
    message: str = None,
    image_path: str = None,
    caption: str = None,
    token: str = None,
    parse_mode: str = "HTML",
) -> dict[str, Union[dict, str]]:
    """
    Send a message and/or image to multiple recipients.

    Args:
        chat_ids: List of target chat IDs.
        message: Text message to send (optional if image_path is provided).
        image_path: Path to image file to send (optional if message is provided).
        caption: Caption for the image.
        token: Bot token. If None, reads from api_keys/telegram_bot_token.txt.
        parse_mode: Message formatting.

    Returns:
        Dict mapping each chat_id to its result or error string.
    """
    if not message and not image_path:
        raise ValueError("[send_to_multiple] Must provide message and/or image_path")

    token = load_token(token)
    results = {}

    for chat_id in chat_ids:
        try:
            if message:
                send_message(chat_id, message, token=token, parse_mode=parse_mode)
            if image_path:
                send_image(chat_id, image_path, caption=caption, token=token, parse_mode=parse_mode)
            results[chat_id] = "ok"
        except Exception as e:
            logger.error(f"[send_to_multiple] Failed for chat_id={chat_id}: {e}")
            results[chat_id] = str(e)

    logger.info(f"[send_to_multiple] Done. {sum(1 for v in results.values() if v == 'ok')}/{len(chat_ids)} succeeded")
    return results

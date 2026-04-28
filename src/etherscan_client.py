import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

BASE_URL = os.getenv("ETHERSCAN_BASE_URL", "https://api.etherscan.io/api")
API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

# Enforce max 5 requests/second with a slightly safer interval.
MIN_CALL_INTERVAL_SECONDS = 0.21
MAX_CALLS_PER_DAY = 100_000
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_MAX_RETRIES = 5

_CACHE_DIR = Path("cache")
_USAGE_FILE = _CACHE_DIR / "api_usage.json"

_RATE_LOCK = threading.Lock()
_LAST_CALL_TS = 0.0

LOGGER = logging.getLogger(__name__)


def set_api_key(api_key: str) -> None:
    global API_KEY
    API_KEY = api_key.strip()


def _utc_date_str() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _load_daily_usage() -> dict[str, Any]:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if not _USAGE_FILE.exists():
        return {"date": _utc_date_str(), "count": 0}

    try:
        with _USAGE_FILE.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if "date" not in data or "count" not in data:
            return {"date": _utc_date_str(), "count": 0}
        return data
    except (OSError, json.JSONDecodeError):
        LOGGER.warning("Failed to read usage file. Resetting daily usage counter.")
        return {"date": _utc_date_str(), "count": 0}


def _save_daily_usage(data: dict[str, Any]) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with _USAGE_FILE.open("w", encoding="utf-8") as fh:
        json.dump(data, fh)


def _increment_and_validate_daily_quota() -> None:
    usage = _load_daily_usage()
    today = _utc_date_str()

    if usage.get("date") != today:
        usage = {"date": today, "count": 0}

    if int(usage.get("count", 0)) >= MAX_CALLS_PER_DAY:
        raise RuntimeError(
            f"Daily API quota reached ({MAX_CALLS_PER_DAY} calls/day). "
            "Stopping to prevent exhaustion."
        )

    usage["count"] = int(usage.get("count", 0)) + 1
    _save_daily_usage(usage)


def _enforce_rate_limit() -> None:
    global _LAST_CALL_TS

    with _RATE_LOCK:
        now = time.time()
        elapsed = now - _LAST_CALL_TS
        if elapsed < MIN_CALL_INTERVAL_SECONDS:
            sleep_for = MIN_CALL_INTERVAL_SECONDS - elapsed
            LOGGER.info("Rate limiting pause: sleeping %.3fs", sleep_for)
            time.sleep(sleep_for)
        _LAST_CALL_TS = time.time()


def call_etherscan(params: dict[str, Any]) -> dict[str, Any]:
    """Single entrypoint for all Etherscan requests with rate limiting and retries."""
    if not API_KEY:
        raise ValueError(
            "Etherscan API key is missing. Set ETHERSCAN_API_KEY or pass --api-key."
        )

    final_params = dict(params)
    final_params["apikey"] = API_KEY

    backoff_seconds = 1.0
    last_error: Exception | None = None

    for attempt in range(1, DEFAULT_MAX_RETRIES + 2):
        try:
            _enforce_rate_limit()
            _increment_and_validate_daily_quota()
            LOGGER.info("API call #%d attempt %d params=%s", attempt, attempt, final_params)

            response = requests.get(BASE_URL, params=final_params, timeout=DEFAULT_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()

            if isinstance(payload, dict):
                # Most Etherscan REST endpoints return status/message/result.
                status = str(payload.get("status", ""))
                message = str(payload.get("message", ""))
                result = payload.get("result")

                # Proxy endpoints can return without status, which is still valid.
                if status == "0":
                    msg_lower = f"{message} {result}".lower()
                    retryable = (
                        "max rate limit" in msg_lower
                        or "busy" in msg_lower
                        or "timeout" in msg_lower
                        or "temporarily unavailable" in msg_lower
                    )
                    if retryable and attempt <= DEFAULT_MAX_RETRIES:
                        LOGGER.warning(
                            "Retryable API response on attempt %d: %s. Backing off %.1fs",
                            attempt,
                            payload,
                            backoff_seconds,
                        )
                        time.sleep(backoff_seconds)
                        backoff_seconds *= 2
                        continue
                return payload

            return {"status": "1", "message": "OK", "result": payload}

        except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt > DEFAULT_MAX_RETRIES:
                break
            LOGGER.warning(
                "Request failed on attempt %d: %s. Backing off %.1fs",
                attempt,
                exc,
                backoff_seconds,
            )
            time.sleep(backoff_seconds)
            backoff_seconds *= 2

    raise RuntimeError(f"Etherscan call failed after retries: {last_error}")

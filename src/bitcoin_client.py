import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

BASE_URL = os.getenv("BITCOIN_API_BASE_URL", "https://blockchain.info")
BLOCKCYPHER_BASE_URL = os.getenv("BLOCKCYPHER_API_BASE_URL", "https://api.blockcypher.com/v1/btc/main")
BLOCKSTREAM_BASE_URL = os.getenv("BLOCKSTREAM_API_BASE_URL", "https://blockstream.info/api")
MEMPOOL_BASE_URL = os.getenv("MEMPOOL_API_BASE_URL", "https://mempool.space/api")
BLOCKCHAIR_BASE_URL = os.getenv("BLOCKCHAIR_API_BASE_URL", "https://api.blockchair.com/bitcoin")

# Match project-wide throttling policy.
MIN_CALL_INTERVAL_SECONDS = 0.21
MAX_CALLS_PER_DAY = 100_000
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_MAX_RETRIES = 5

_CACHE_DIR = Path("cache")
_USAGE_FILE = _CACHE_DIR / "api_usage.json"

_RATE_LOCK = threading.Lock()
_LAST_CALL_TS = 0.0

LOGGER = logging.getLogger(__name__)


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


def call_blockchain_info(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Single entrypoint for blockchain.info requests with retries and rate limiting."""
    final_path = path if path.startswith("/") else f"/{path}"
    url = f"{BASE_URL}{final_path}"

    backoff_seconds = 1.0
    last_error: Exception | None = None

    for attempt in range(1, DEFAULT_MAX_RETRIES + 2):
        try:
            _enforce_rate_limit()
            _increment_and_validate_daily_quota()
            LOGGER.info("API call attempt %d url=%s params=%s", attempt, url, params or {})

            response = requests.get(url, params=params or {}, timeout=DEFAULT_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()

            if isinstance(payload, dict):
                return payload
            return {"result": payload}

        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 404:
                LOGGER.warning("Resource not found at %s", url)
                return {}
            if status_code in {400, 403, 429}:
                raise RuntimeError(f"HTTP {status_code} provider failure at {url}") from exc
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

    raise RuntimeError(f"Blockchain.info call failed after retries: {last_error}")


def call_blockcypher(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Single entrypoint for BlockCypher requests with retries and rate limiting."""
    final_path = path if path.startswith("/") else f"/{path}"
    url = f"{BLOCKCYPHER_BASE_URL}{final_path}"

    backoff_seconds = 1.0
    last_error: Exception | None = None

    for attempt in range(1, DEFAULT_MAX_RETRIES + 2):
        try:
            _enforce_rate_limit()
            _increment_and_validate_daily_quota()
            LOGGER.info("API call attempt %d url=%s params=%s", attempt, url, params or {})

            response = requests.get(url, params=params or {}, timeout=DEFAULT_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()

            if isinstance(payload, dict):
                return payload
            return {"result": payload}

        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 404:
                LOGGER.warning("Resource not found at %s", url)
                return {}
            if status_code in {400, 403, 429}:
                raise RuntimeError(f"HTTP {status_code} provider failure at {url}") from exc
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

    raise RuntimeError(f"BlockCypher call failed after retries: {last_error}")


def call_blockstream(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Single entrypoint for Blockstream requests with retries and rate limiting."""
    final_path = path if path.startswith("/") else f"/{path}"
    url = f"{BLOCKSTREAM_BASE_URL}{final_path}"

    backoff_seconds = 1.0
    last_error: Exception | None = None

    for attempt in range(1, DEFAULT_MAX_RETRIES + 2):
        try:
            _enforce_rate_limit()
            _increment_and_validate_daily_quota()
            LOGGER.info("API call attempt %d url=%s params=%s", attempt, url, params or {})

            response = requests.get(url, params=params or {}, timeout=DEFAULT_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()

            if isinstance(payload, dict):
                return payload
            if isinstance(payload, list):
                return {"result": payload}
            return {"result": payload}

        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 404:
                LOGGER.warning("Resource not found at %s", url)
                return {}
            if status_code in {400, 403, 429}:
                raise RuntimeError(f"HTTP {status_code} provider failure at {url}") from exc
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

    raise RuntimeError(f"Blockstream call failed after retries: {last_error}")


def call_mempool(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Single entrypoint for mempool.space requests with retries and rate limiting."""
    final_path = path if path.startswith("/") else f"/{path}"
    url = f"{MEMPOOL_BASE_URL}{final_path}"

    backoff_seconds = 1.0
    last_error: Exception | None = None

    for attempt in range(1, DEFAULT_MAX_RETRIES + 2):
        try:
            _enforce_rate_limit()
            _increment_and_validate_daily_quota()
            LOGGER.info("API call attempt %d url=%s params=%s", attempt, url, params or {})

            response = requests.get(url, params=params or {}, timeout=DEFAULT_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()

            if isinstance(payload, dict):
                return payload
            if isinstance(payload, list):
                return {"result": payload}
            return {"result": payload}

        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 404:
                LOGGER.warning("Resource not found at %s", url)
                return {}
            if status_code in {400, 402, 403, 429}:
                raise RuntimeError(f"HTTP {status_code} provider failure at {url}") from exc
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

    raise RuntimeError(f"Mempool call failed after retries: {last_error}")


def call_blockchair(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Single entrypoint for Blockchair requests with retries and rate limiting."""
    final_path = path if path.startswith("/") else f"/{path}"
    url = f"{BLOCKCHAIR_BASE_URL}{final_path}"

    backoff_seconds = 1.0
    last_error: Exception | None = None

    for attempt in range(1, DEFAULT_MAX_RETRIES + 2):
        try:
            _enforce_rate_limit()
            _increment_and_validate_daily_quota()
            LOGGER.info("API call attempt %d url=%s params=%s", attempt, url, params or {})

            response = requests.get(url, params=params or {}, timeout=DEFAULT_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()

            if isinstance(payload, dict):
                return payload
            if isinstance(payload, list):
                return {"result": payload}
            return {"result": payload}

        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 404:
                LOGGER.warning("Resource not found at %s", url)
                return {}
            if status_code in {400, 402, 403, 429}:
                raise RuntimeError(f"HTTP {status_code} provider failure at {url}") from exc
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

    raise RuntimeError(f"Blockchair call failed after retries: {last_error}")

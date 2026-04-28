import argparse
from collections import deque
import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import networkx as nx
import pandas as pd

from src.bitcoin_client import (
    call_blockchain_info,
    call_blockchair,
    call_blockcypher,
    call_blockstream,
    call_mempool,
)
from src.etherscan_client import call_etherscan, set_api_key

LOGGER = logging.getLogger(__name__)

CACHE_DIR = Path("cache")
ETH_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
BTC_BASE58_RE = re.compile(r"^[13][a-km-zA-HJ-NP-Z1-9]{25,34}$")
BTC_BECH32_RE = re.compile(r"^bc1[ac-hj-np-z02-9]{11,71}$")
TX_HASH_RE = re.compile(r"^0x[a-fA-F0-9]{64}$")
RAW_TX_HASH_RE = re.compile(r"^[a-fA-F0-9]{64}$")
ETH_DEFAULT_PAGE_SIZE = 1000
BTC_DEFAULT_PAGE_SIZE = 100
MAX_WALLET_TRANSACTIONS = 10_000
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def ensure_cache_dir() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def normalize_wallet(value: str, chain: str = "ethereum") -> str:
    raw = value.strip()
    if chain == "ethereum":
        return raw.lower()
    # Base58 Bitcoin addresses are case-sensitive; bech32 is lowercase.
    if raw.lower().startswith("bc1"):
        return raw.lower()
    return raw


def _is_valid_base58check_address(address: str) -> bool:
    if not BTC_BASE58_RE.match(address):
        return False

    num = 0
    for char in address:
        idx = BASE58_ALPHABET.find(char)
        if idx < 0:
            return False
        num = num * 58 + idx

    decoded = num.to_bytes((num.bit_length() + 7) // 8, "big") if num > 0 else b""
    leading_zeros = len(address) - len(address.lstrip("1"))
    payload = (b"\x00" * leading_zeros) + decoded

    if len(payload) < 5:
        return False

    body = payload[:-4]
    checksum = payload[-4:]
    expected = hashlib.sha256(hashlib.sha256(body).digest()).digest()[:4]
    return checksum == expected


def is_valid_wallet(value: str, chain: str) -> bool:
    normalized = normalize_wallet(value, chain)
    if chain == "ethereum":
        return bool(ETH_ADDRESS_RE.match(normalized))
    return bool(_is_valid_base58check_address(value.strip()) or BTC_BECH32_RE.match(normalized))


def load_json_file(path: str) -> Any:
    if not path:
        return []
    if not os.path.exists(path):
        LOGGER.warning("Input file not found: %s", path)
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _extract_strings(payload: Any) -> list[str]:
    if isinstance(payload, list):
        return [str(x).strip() for x in payload if isinstance(x, str)]
    if isinstance(payload, dict):
        values: list[str] = []
        for _, item in payload.items():
            if isinstance(item, list):
                values.extend(str(x).strip() for x in item if isinstance(x, str))
            elif isinstance(item, str):
                values.append(item.strip())
        return values
    return []


def load_wallets(wallets_path: str, chain: str) -> set[str]:
    raw = _extract_strings(load_json_file(wallets_path))
    wallets = {normalize_wallet(x, chain) for x in raw if is_valid_wallet(x, chain)}
    LOGGER.info("Loaded %d unique wallets", len(wallets))
    return wallets


def load_tx_hashes(txs_path: str, chain: str) -> set[str]:
    raw = _extract_strings(load_json_file(txs_path))
    tx_hashes: set[str] = set()
    for item in raw:
        candidate = item.strip().lower()
        if chain == "ethereum":
            if TX_HASH_RE.match(candidate):
                tx_hashes.add(candidate)
            elif RAW_TX_HASH_RE.match(candidate):
                tx_hashes.add(f"0x{candidate}")
        else:
            if TX_HASH_RE.match(candidate):
                tx_hashes.add(candidate[2:])
            elif RAW_TX_HASH_RE.match(candidate):
                tx_hashes.add(candidate)
    LOGGER.info("Loaded %d unique transaction hashes", len(tx_hashes))
    return tx_hashes


def parse_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        if isinstance(value, str) and value.lower().startswith("0x"):
            return int(value, 16)
        return int(value)
    except (TypeError, ValueError):
        return 0


def parse_timestamp(value: Any) -> int:
    # Etherscan timestamps are unix seconds in string form.
    return parse_int(value)


def timestamp_to_iso(ts: int) -> str:
    if ts <= 0:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def parse_iso_timestamp(value: Any) -> int:
    if not isinstance(value, str) or not value.strip():
        return 0
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


def normalize_blockcypher_tx(tx: dict[str, Any]) -> dict[str, Any]:
    inputs: list[dict[str, Any]] = []
    for input_item in tx.get("inputs", []):
        if not isinstance(input_item, dict):
            continue
        for address in input_item.get("addresses", []) or []:
            inputs.append({"prev_out": {"addr": str(address)}})

    outputs: list[dict[str, Any]] = []
    for output_item in tx.get("outputs", []):
        if not isinstance(output_item, dict):
            continue
        value = parse_int(output_item.get("value"))
        for address in output_item.get("addresses", []) or []:
            outputs.append({"addr": str(address), "value": value})

    return {
        "hash": tx.get("hash", ""),
        "inputs": inputs,
        "out": outputs,
        "time": parse_iso_timestamp(tx.get("confirmed")),
    }


def normalize_blockstream_tx(tx: dict[str, Any]) -> dict[str, Any]:
    inputs: list[dict[str, Any]] = []
    for input_item in tx.get("vin", []):
        if not isinstance(input_item, dict):
            continue
        prevout = input_item.get("prevout", {}) if isinstance(input_item.get("prevout"), dict) else {}
        addr = str(prevout.get("scriptpubkey_address", ""))
        if addr:
            inputs.append({"prev_out": {"addr": addr}})

    outputs: list[dict[str, Any]] = []
    for output_item in tx.get("vout", []):
        if not isinstance(output_item, dict):
            continue
        addr = str(output_item.get("scriptpubkey_address", ""))
        if not addr:
            continue
        outputs.append({"addr": addr, "value": parse_int(output_item.get("value"))})

    status = tx.get("status", {}) if isinstance(tx.get("status"), dict) else {}
    return {
        "hash": tx.get("txid", ""),
        "inputs": inputs,
        "out": outputs,
        "time": parse_int(status.get("block_time")),
    }


def normalize_blockchair_tx(tx: dict[str, Any]) -> dict[str, Any]:
    tx_meta = tx.get("transaction", {}) if isinstance(tx.get("transaction"), dict) else {}

    inputs: list[dict[str, Any]] = []
    for input_item in tx.get("inputs", []):
        if not isinstance(input_item, dict):
            continue
        addr = str(input_item.get("recipient", ""))
        if addr:
            inputs.append({"prev_out": {"addr": addr}})

    outputs: list[dict[str, Any]] = []
    for output_item in tx.get("outputs", []):
        if not isinstance(output_item, dict):
            continue
        addr = str(output_item.get("recipient", ""))
        if not addr:
            continue
        outputs.append({"addr": addr, "value": parse_int(output_item.get("value"))})

    return {
        "hash": tx_meta.get("hash", ""),
        "inputs": inputs,
        "out": outputs,
        "time": parse_iso_timestamp(tx_meta.get("time")),
    }


def get_wallet_transactions_blockcypher(wallet: str, max_wallet_transactions: int) -> list[dict[str, Any]]:
    all_txs: list[dict[str, Any]] = []
    tx_start = 0

    while True:
        response = call_blockcypher(
            f"/addrs/{wallet}/full",
            params={"txlimit": BTC_DEFAULT_PAGE_SIZE, "txstart": tx_start},
        )

        result = response.get("txs", [])
        if not isinstance(result, list) or not result:
            break

        normalized = [normalize_blockcypher_tx(tx) for tx in result if isinstance(tx, dict)]
        all_txs.extend(normalized)

        if len(all_txs) >= max_wallet_transactions:
            LOGGER.warning(
                "Stopping early for %s after %d txs (threshold=%d) to protect API budget",
                wallet,
                len(all_txs),
                max_wallet_transactions,
            )
            break

        if len(result) < BTC_DEFAULT_PAGE_SIZE:
            break

        tx_start += BTC_DEFAULT_PAGE_SIZE

    return all_txs


def get_wallet_transactions_blockstream(wallet: str, max_wallet_transactions: int) -> list[dict[str, Any]]:
    all_txs: list[dict[str, Any]] = []
    last_seen_txid = ""

    while True:
        path = f"/address/{wallet}/txs" if not last_seen_txid else f"/address/{wallet}/txs/chain/{last_seen_txid}"
        response = call_blockstream(path)
        result = response.get("result", [])
        if not isinstance(result, list) or not result:
            break

        normalized = [normalize_blockstream_tx(tx) for tx in result if isinstance(tx, dict)]
        all_txs.extend(normalized)

        if len(all_txs) >= max_wallet_transactions:
            LOGGER.warning(
                "Stopping early for %s after %d txs (threshold=%d) to protect API budget",
                wallet,
                len(all_txs),
                max_wallet_transactions,
            )
            break

        last_item = result[-1] if isinstance(result[-1], dict) else {}
        last_seen_txid = str(last_item.get("txid", ""))
        if not last_seen_txid or len(result) < 25:
            break

    return all_txs


def get_wallet_transactions_mempool(wallet: str, max_wallet_transactions: int) -> list[dict[str, Any]]:
    all_txs: list[dict[str, Any]] = []
    last_seen_txid = ""

    while True:
        path = f"/address/{wallet}/txs" if not last_seen_txid else f"/address/{wallet}/txs/chain/{last_seen_txid}"
        response = call_mempool(path)
        result = response.get("result", [])
        if not isinstance(result, list) or not result:
            break

        normalized = [normalize_blockstream_tx(tx) for tx in result if isinstance(tx, dict)]
        all_txs.extend(normalized)

        if len(all_txs) >= max_wallet_transactions:
            LOGGER.warning(
                "Stopping early for %s after %d txs (threshold=%d) to protect API budget",
                wallet,
                len(all_txs),
                max_wallet_transactions,
            )
            break

        last_item = result[-1] if isinstance(result[-1], dict) else {}
        last_seen_txid = str(last_item.get("txid", ""))
        if not last_seen_txid or len(result) < 25:
            break

    return all_txs


def get_wallet_transactions_blockchair(wallet: str, max_wallet_transactions: int) -> list[dict[str, Any]]:
    all_txs: list[dict[str, Any]] = []
    offset = 0

    while True:
        response = call_blockchair(
            f"/dashboards/address/{wallet}",
            params={"offset": offset, "limit": BTC_DEFAULT_PAGE_SIZE},
        )
        data = response.get("data", {}) if isinstance(response.get("data"), dict) else {}
        if not data:
            break

        first_key = next(iter(data.keys()))
        wallet_data = data.get(first_key, {}) if isinstance(data.get(first_key), dict) else {}
        tx_ids = wallet_data.get("transactions", [])
        if not isinstance(tx_ids, list) or not tx_ids:
            break

        for txid in tx_ids:
            txid_str = str(txid).strip()
            if not txid_str:
                continue
            tx_resp = call_blockchair(f"/dashboards/transaction/{txid_str}")
            tx_data = tx_resp.get("data", {}) if isinstance(tx_resp.get("data"), dict) else {}
            if not tx_data:
                continue
            tx_key = next(iter(tx_data.keys()))
            tx_obj = tx_data.get(tx_key, {}) if isinstance(tx_data.get(tx_key), dict) else {}
            if not tx_obj:
                continue
            normalized = normalize_blockchair_tx(tx_obj)
            if normalized.get("hash"):
                all_txs.append(normalized)

            if len(all_txs) >= max_wallet_transactions:
                break

        if len(all_txs) >= max_wallet_transactions:
            LOGGER.warning(
                "Stopping early for %s after %d txs (threshold=%d) to protect API budget",
                wallet,
                len(all_txs),
                max_wallet_transactions,
            )
            break

        if len(tx_ids) < BTC_DEFAULT_PAGE_SIZE:
            break
        offset += BTC_DEFAULT_PAGE_SIZE

    return all_txs


def get_wallet_cache_path(wallet: str, chain: str) -> Path:
    if chain == "ethereum":
        return CACHE_DIR / f"{wallet}.json"
    safe_wallet = re.sub(r"[^a-zA-Z0-9_-]", "_", wallet)
    return CACHE_DIR / f"btc_{safe_wallet}.json"


def get_wallet_transactions(wallet: str, max_wallet_transactions: int, chain: str) -> list[dict[str, Any]]:
    cache_path = get_wallet_cache_path(wallet, chain)
    if cache_path.exists():
        LOGGER.info("Cache hit for wallet %s", wallet)
        with cache_path.open("r", encoding="utf-8") as fh:
            cached = json.load(fh)
        return cached.get("transactions", [])

    LOGGER.info("Cache miss for wallet %s", wallet)

    all_txs: list[dict[str, Any]] = []

    if chain == "ethereum":
        page = 1
        while True:
            response = call_etherscan(
                {
                    "module": "account",
                    "action": "txlist",
                    "address": wallet,
                    "startblock": 0,
                    "endblock": 99999999,
                    "page": page,
                    "offset": ETH_DEFAULT_PAGE_SIZE,
                    "sort": "asc",
                }
            )

            status = str(response.get("status", ""))
            result = response.get("result", [])

            if status == "0":
                msg = str(response.get("message", "")).lower()
                if "no transactions" in msg:
                    result = []
                else:
                    LOGGER.warning("Unexpected txlist response for %s: %s", wallet, response)
                    break

            if not isinstance(result, list) or not result:
                break

            all_txs.extend(result)

            if len(all_txs) > max_wallet_transactions:
                LOGGER.warning(
                    "Stopping early for %s after %d txs (threshold=%d) to protect API budget",
                    wallet,
                    len(all_txs),
                    max_wallet_transactions,
                )
                break

            if len(result) < ETH_DEFAULT_PAGE_SIZE:
                break

            page += 1
    else:
        offset = 0
        expected_total: int | None = None
        while True:
            try:
                response = call_blockchain_info(
                    f"/rawaddr/{wallet}",
                    params={"offset": offset, "limit": BTC_DEFAULT_PAGE_SIZE},
                )
            except RuntimeError as exc:
                LOGGER.warning(
                    "Blockchain.info failed for %s (%s), switching to BlockCypher /addrs/{address}/full",
                    wallet,
                    exc,
                )
                try:
                    all_txs.extend(get_wallet_transactions_blockcypher(wallet, max_wallet_transactions))
                except RuntimeError as fallback_exc:
                    LOGGER.warning(
                        "BlockCypher failed for %s (%s), switching to Blockstream /address/{address}/txs",
                        wallet,
                        fallback_exc,
                    )
                    try:
                        all_txs.extend(get_wallet_transactions_blockstream(wallet, max_wallet_transactions))
                    except RuntimeError as blockstream_exc:
                        LOGGER.warning(
                            "Blockstream failed for %s (%s), switching to mempool.space /address/{address}/txs",
                            wallet,
                            blockstream_exc,
                        )
                        try:
                            all_txs.extend(get_wallet_transactions_mempool(wallet, max_wallet_transactions))
                        except RuntimeError as mempool_exc:
                            LOGGER.warning(
                                "Mempool failed for %s (%s), switching to Blockchair dashboards/address",
                                wallet,
                                mempool_exc,
                            )
                            try:
                                all_txs.extend(get_wallet_transactions_blockchair(wallet, max_wallet_transactions))
                            except RuntimeError as blockchair_exc:
                                raise RuntimeError(
                                    f"All bitcoin providers failed for wallet {wallet}: {blockchair_exc}"
                                ) from blockchair_exc
                break

            if expected_total is None:
                expected_total = parse_int(response.get("n_tx"))
                if expected_total > max_wallet_transactions:
                    LOGGER.warning(
                        "Stopping early for %s with n_tx=%d (threshold=%d) to protect API budget",
                        wallet,
                        expected_total,
                        max_wallet_transactions,
                    )

            result = response.get("txs", [])
            if not isinstance(result, list) or not result:
                break

            all_txs.extend(result)
            if len(all_txs) >= max_wallet_transactions:
                break

            if len(result) < BTC_DEFAULT_PAGE_SIZE:
                break

            if expected_total is not None and offset + BTC_DEFAULT_PAGE_SIZE >= min(expected_total, max_wallet_transactions):
                break

            offset += BTC_DEFAULT_PAGE_SIZE

    payload = {
        "wallet": wallet,
        "tx_count": len(all_txs),
        "transactions": all_txs,
        "cached_at": datetime.now(timezone.utc).isoformat(),
    }
    with cache_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh)

    return all_txs
def fetch_transaction_details(tx_hash: str, chain: str) -> dict[str, Any] | None:
    if chain == "ethereum":
        response = call_etherscan(
            {
                "module": "proxy",
                "action": "eth_getTransactionByHash",
                "txhash": tx_hash,
            }
        )

        result = response.get("result")
        if not isinstance(result, dict):
            LOGGER.warning("No tx details for hash %s", tx_hash)
            return None
        return result

    try:
        response = call_blockchain_info(f"/rawtx/{tx_hash}")
    except RuntimeError as exc:
        LOGGER.warning("Blockchain.info tx lookup failed for %s: %s", tx_hash, exc)
        try:
            bc_response = call_blockcypher(f"/txs/{tx_hash}")
            if isinstance(bc_response, dict) and bc_response:
                return normalize_blockcypher_tx(bc_response)
        except RuntimeError as fallback_exc:
            LOGGER.warning("BlockCypher tx lookup failed for %s: %s", tx_hash, fallback_exc)
            try:
                bs_response = call_blockstream(f"/tx/{tx_hash}")
                if isinstance(bs_response, dict) and bs_response:
                    return normalize_blockstream_tx(bs_response)
            except RuntimeError as blockstream_exc:
                LOGGER.warning("Blockstream tx lookup failed for %s: %s", tx_hash, blockstream_exc)
                try:
                    mempool_response = call_mempool(f"/tx/{tx_hash}")
                    if isinstance(mempool_response, dict) and mempool_response:
                        return normalize_blockstream_tx(mempool_response)
                except RuntimeError as mempool_exc:
                    LOGGER.warning("Mempool tx lookup failed for %s: %s", tx_hash, mempool_exc)
                    try:
                        blockchair_response = call_blockchair(f"/dashboards/transaction/{tx_hash}")
                        data = blockchair_response.get("data", {}) if isinstance(blockchair_response.get("data"), dict) else {}
                        if data:
                            first_key = next(iter(data.keys()))
                            tx_obj = data.get(first_key, {}) if isinstance(data.get(first_key), dict) else {}
                            if tx_obj:
                                return normalize_blockchair_tx(tx_obj)
                    except RuntimeError:
                        pass
        LOGGER.warning("No tx details for hash %s", tx_hash)
        return None

    if not isinstance(response, dict):
        LOGGER.warning("No tx details for hash %s", tx_hash)
        return None
    return response


def _upsert_edge(graph: nx.DiGraph, src: str, dst: str, tx_hash: str, value: int, timestamp: int) -> None:
    if graph.has_edge(src, dst):
        existing = graph[src][dst]
        existing["value"] = parse_int(existing.get("value")) + value
        existing["tx_count"] = parse_int(existing.get("tx_count")) + 1
        existing["timestamp"] = max(parse_int(existing.get("timestamp")), timestamp)
        existing_hashes = str(existing.get("tx_hashes", ""))
        existing["tx_hashes"] = ",".join([h for h in [existing_hashes, tx_hash] if h])
    else:
        graph.add_edge(
            src,
            dst,
            tx_hash=tx_hash,
            tx_hashes=tx_hash,
            tx_count=1,
            value=value,
            timestamp=timestamp,
        )


def _extract_wallets_from_tx(tx_detail: dict[str, Any], chain: str) -> set[str]:
    wallets: set[str] = set()
    if chain == "ethereum":
        src = normalize_wallet(str(tx_detail.get("from", "")), chain)
        dst = normalize_wallet(str(tx_detail.get("to", "")), chain)
        if is_valid_wallet(src, chain):
            wallets.add(src)
        if is_valid_wallet(dst, chain):
            wallets.add(dst)
        return wallets

    for item in tx_detail.get("inputs", []):
        prev_out = item.get("prev_out", {}) if isinstance(item, dict) else {}
        src = normalize_wallet(str(prev_out.get("addr", "")), chain)
        if is_valid_wallet(src, chain):
            wallets.add(src)
    for item in tx_detail.get("out", []):
        dst = normalize_wallet(str(item.get("addr", "")), chain) if isinstance(item, dict) else ""
        if is_valid_wallet(dst, chain):
            wallets.add(dst)
    return wallets


def build_graph(
    wallets_to_fetch: set[str],
    seed_transactions: list[dict[str, Any]],
    chain: str,
) -> tuple[nx.DiGraph, dict[str, dict[str, Any]]]:
    graph = nx.DiGraph()
    tx_by_hash: dict[str, dict[str, Any]] = {}

    for wallet in wallets_to_fetch:
        graph.add_node(wallet)

    for tx in seed_transactions:
        tx_hash = str(tx.get("hash", tx.get("txid", ""))).lower()
        if tx_hash:
            tx_by_hash[tx_hash] = tx

    for wallet in wallets_to_fetch:
        txs = get_wallet_transactions(wallet, MAX_WALLET_TRANSACTIONS, chain)
        for tx in txs:
            tx_hash = str(tx.get("hash", tx.get("txid", ""))).lower()
            if tx_hash:
                tx_by_hash[tx_hash] = tx

    for tx_hash, tx in tx_by_hash.items():
        if chain == "ethereum":
            src = normalize_wallet(str(tx.get("from", "")), chain)
            dst = normalize_wallet(str(tx.get("to", "")), chain)
            if not (is_valid_wallet(src, chain) and is_valid_wallet(dst, chain)):
                continue
            value = parse_int(tx.get("value"))
            timestamp = parse_timestamp(tx.get("timeStamp"))
            graph.add_node(src)
            graph.add_node(dst)
            _upsert_edge(graph, src, dst, tx_hash, value, timestamp)
            continue

        timestamp = parse_timestamp(tx.get("time"))
        input_addrs = []
        for item in tx.get("inputs", []):
            prev_out = item.get("prev_out", {}) if isinstance(item, dict) else {}
            src = normalize_wallet(str(prev_out.get("addr", "")), chain)
            if is_valid_wallet(src, chain):
                input_addrs.append(src)

        output_items = []
        for item in tx.get("out", []):
            if not isinstance(item, dict):
                continue
            dst = normalize_wallet(str(item.get("addr", "")), chain)
            if not is_valid_wallet(dst, chain):
                continue
            output_items.append((dst, parse_int(item.get("value"))))

        if not input_addrs or not output_items:
            continue

        for src in sorted(set(input_addrs)):
            graph.add_node(src)
            for dst, value in output_items:
                graph.add_node(dst)
                _upsert_edge(graph, src, dst, tx_hash, value, timestamp)

    return graph, tx_by_hash


def classify_wallet(total_in: int, total_out: int, in_degree: int, out_degree: int, tx_count: int) -> str:
    if tx_count == 0:
        return "isolated"
    if in_degree > 0 and out_degree == 0 and total_in > 0:
        return "collector"
    if out_degree > 0 and in_degree == 0 and total_out > 0:
        return "distributor"
    return "transit"


def extract_features(graph: nx.DiGraph, target_wallets: set[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for wallet in sorted(target_wallets):
        in_edges = list(graph.in_edges(wallet, data=True))
        out_edges = list(graph.out_edges(wallet, data=True))

        total_in = sum(parse_int(data.get("value")) for _, _, data in in_edges)
        total_out = sum(parse_int(data.get("value")) for _, _, data in out_edges)

        timestamps = [parse_timestamp(data.get("timestamp")) for _, _, data in in_edges + out_edges]
        timestamps = [ts for ts in timestamps if ts > 0]

        if timestamps:
            first_seen = min(timestamps)
            last_seen = max(timestamps)
            active_duration = last_seen - first_seen
        else:
            first_seen = 0
            last_seen = 0
            active_duration = 0

        in_degree = graph.in_degree(wallet)
        out_degree = graph.out_degree(wallet)
        transaction_count = len(in_edges) + len(out_edges)

        label = classify_wallet(total_in, total_out, in_degree, out_degree, transaction_count)

        rows.append(
            {
                "wallet": wallet,
                "total_in": total_in,
                "total_out": total_out,
                "in_degree": in_degree,
                "out_degree": out_degree,
                "transaction_count": transaction_count,
                "first_seen": timestamp_to_iso(first_seen),
                "last_seen": timestamp_to_iso(last_seen),
                "active_duration": active_duration,
                "classification": label,
            }
        )

    return pd.DataFrame(rows)


def satoshi_to_octets_little_endian(satoshi_value: int) -> tuple[int, int]:
    # Malware encoding commonly used the least-significant 16 bits of value.
    value16 = satoshi_value & 0xFFFF
    low = value16 & 0xFF
    high = (value16 >> 8) & 0xFF
    return low, high


def collect_signal_events(
    tx_by_hash: dict[str, dict[str, Any]],
    chain: str,
    target_wallets: set[str],
) -> dict[str, list[dict[str, Any]]]:
    events: dict[str, list[dict[str, Any]]] = {wallet: [] for wallet in target_wallets}

    for tx_hash, tx in tx_by_hash.items():
        if chain == "ethereum":
            dst = normalize_wallet(str(tx.get("to", "")), chain)
            if dst not in target_wallets:
                continue
            events[dst].append(
                {
                    "tx_hash": tx_hash,
                    "value": parse_int(tx.get("value")),
                    "timestamp": parse_timestamp(tx.get("timeStamp")),
                    "sender": normalize_wallet(str(tx.get("from", "")), chain),
                }
            )
            continue

        timestamp = parse_timestamp(tx.get("time"))
        senders: list[str] = []
        for input_item in tx.get("inputs", []):
            prev_out = input_item.get("prev_out", {}) if isinstance(input_item, dict) else {}
            sender = normalize_wallet(str(prev_out.get("addr", "")), chain)
            if sender and is_valid_wallet(sender, chain):
                senders.append(sender)

        sender_text = ",".join(sorted(set(senders)))
        for output_item in tx.get("out", []):
            if not isinstance(output_item, dict):
                continue
            dst = normalize_wallet(str(output_item.get("addr", "")), chain)
            if dst not in target_wallets:
                continue
            events[dst].append(
                {
                    "tx_hash": tx_hash,
                    "value": parse_int(output_item.get("value")),
                    "timestamp": timestamp,
                    "sender": sender_text,
                }
            )

    for wallet in events:
        events[wallet].sort(key=lambda item: item.get("timestamp", 0), reverse=True)
    return events


def build_c2_signals_dataframe(
    tx_by_hash: dict[str, dict[str, Any]],
    chain: str,
    monitored_wallets: set[str],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    events_by_wallet = collect_signal_events(tx_by_hash, chain, monitored_wallets)

    for wallet in sorted(monitored_wallets):
        events = events_by_wallet.get(wallet, [])
        if not events:
            rows.append(
                {
                    "wallet": wallet,
                    "signal_event_count": 0,
                    "candidate_ipv4": "",
                    "latest_tx_hash": "",
                    "latest_value_satoshi": 0,
                    "latest_octet3": "",
                    "latest_octet4": "",
                    "previous_tx_hash": "",
                    "previous_value_satoshi": 0,
                    "previous_octet1": "",
                    "previous_octet2": "",
                    "latest_sender": "",
                    "previous_sender": "",
                    "latest_seen": "",
                    "previous_seen": "",
                }
            )
            continue

        latest = events[0]
        octet3, octet4 = satoshi_to_octets_little_endian(parse_int(latest.get("value")))

        row = {
            "wallet": wallet,
            "signal_event_count": len(events),
            "candidate_ipv4": "",
            "latest_tx_hash": str(latest.get("tx_hash", "")),
            "latest_value_satoshi": parse_int(latest.get("value")),
            "latest_octet3": octet3,
            "latest_octet4": octet4,
            "previous_tx_hash": "",
            "previous_value_satoshi": 0,
            "previous_octet1": "",
            "previous_octet2": "",
            "latest_sender": str(latest.get("sender", "")),
            "previous_sender": "",
            "latest_seen": timestamp_to_iso(parse_int(latest.get("timestamp"))),
            "previous_seen": "",
        }

        if len(events) >= 2:
            previous = events[1]
            octet1, octet2 = satoshi_to_octets_little_endian(parse_int(previous.get("value")))
            row["candidate_ipv4"] = f"{octet1}.{octet2}.{octet3}.{octet4}"
            row["previous_tx_hash"] = str(previous.get("tx_hash", ""))
            row["previous_value_satoshi"] = parse_int(previous.get("value"))
            row["previous_octet1"] = octet1
            row["previous_octet2"] = octet2
            row["previous_sender"] = str(previous.get("sender", ""))
            row["previous_seen"] = timestamp_to_iso(parse_int(previous.get("timestamp")))

        rows.append(row)

    return pd.DataFrame(rows)


def build_related_wallets_dataframe(
    graph: nx.DiGraph,
    seed_wallets: set[str],
    related_depth: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    undirected = graph.to_undirected(as_view=True)

    for seed in sorted(seed_wallets):
        if seed not in graph:
            continue

        visited: dict[str, int] = {seed: 0}
        queue: deque[str] = deque([seed])

        while queue:
            current = queue.popleft()
            current_depth = visited[current]
            if current_depth >= related_depth:
                continue

            for neighbor in undirected.neighbors(current):
                if neighbor in visited:
                    continue
                visited[neighbor] = current_depth + 1
                queue.append(neighbor)

        for related_wallet, hop_distance in visited.items():
            if related_wallet == seed:
                continue

            direct_out = graph.has_edge(seed, related_wallet)
            direct_in = graph.has_edge(related_wallet, seed)
            if direct_in and direct_out:
                relation_type = "bidirectional"
            elif direct_out:
                relation_type = "outgoing"
            elif direct_in:
                relation_type = "incoming"
            else:
                relation_type = "indirect"

            direct_tx_count = 0
            direct_value_sum = 0
            if direct_out:
                edge = graph[seed][related_wallet]
                direct_tx_count += parse_int(edge.get("tx_count", 1))
                direct_value_sum += parse_int(edge.get("value"))
            if direct_in:
                edge = graph[related_wallet][seed]
                direct_tx_count += parse_int(edge.get("tx_count", 1))
                direct_value_sum += parse_int(edge.get("value"))

            rows.append(
                {
                    "seed_wallet": seed,
                    "related_wallet": related_wallet,
                    "hop_distance": hop_distance,
                    "relation_type": relation_type,
                    "direct_tx_count": direct_tx_count,
                    "direct_value_sum": direct_value_sum,
                }
            )

    return pd.DataFrame(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wallet graph analysis for Ethereum or Bitcoin")
    parser.add_argument("--wallets", default="data/wallets.json", help="Path to wallets.json")
    parser.add_argument("--txs", default="data/txs.json", help="Path to txs.json")
    parser.add_argument("--chain", choices=["ethereum", "bitcoin"], default="bitcoin", help="Blockchain to analyze")
    parser.add_argument("--api-key", default=os.getenv("ETHERSCAN_API_KEY", ""), help="Etherscan API key (ethereum only)")
    parser.add_argument("--output-dir", default=".", help="Output directory")
    parser.add_argument(
        "--max-wallet-txs",
        type=int,
        default=MAX_WALLET_TRANSACTIONS,
        help="Early stop threshold for tx count per wallet",
    )
    parser.add_argument(
        "--related-depth",
        type=int,
        default=1,
        help="Hop depth for related wallet chaining from seed wallets",
    )
    return parser.parse_args()


def main() -> None:
    global MAX_WALLET_TRANSACTIONS

    args = parse_args()
    configure_logging()
    ensure_cache_dir()

    if args.chain == "ethereum" and not args.api_key:
        raise SystemExit("Missing Etherscan API key. Use --api-key or ETHERSCAN_API_KEY.")

    if args.chain == "ethereum":
        set_api_key(args.api_key)
    MAX_WALLET_TRANSACTIONS = int(args.max_wallet_txs)

    wallets = load_wallets(args.wallets, args.chain)
    tx_hashes = load_tx_hashes(args.txs, args.chain)

    if not wallets:
        if args.chain == "ethereum":
            LOGGER.warning(
                "No valid Ethereum wallet addresses found in %s. "
                "Expected addresses like 0x + 40 hex chars.",
                args.wallets,
            )
        else:
            LOGGER.warning(
                "No valid Bitcoin wallet addresses found in %s. "
                "Expected base58 (1/3...) or bech32 (bc1...) addresses.",
                args.wallets,
            )

    seed_transactions: list[dict[str, Any]] = []
    extra_wallets: set[str] = set()

    for tx_hash in sorted(tx_hashes):
        tx_detail = fetch_transaction_details(tx_hash, args.chain)
        if not tx_detail:
            continue
        extra_wallets |= _extract_wallets_from_tx(tx_detail, args.chain)
        seed_transactions.append(tx_detail)

    # Depth=1 only: fetch txlists only for this single-level wallet set.
    wallets_to_fetch = set(wallets) | extra_wallets

    if tx_hashes and not extra_wallets:
        if args.chain == "ethereum":
            LOGGER.warning(
                "No wallets were resolved from transaction hashes. "
                "These tx hashes may not belong to Ethereum mainnet or may be invalid for Etherscan."
            )
        else:
            LOGGER.warning(
                "No wallets were resolved from transaction hashes. "
                "These tx hashes may be invalid for blockchain.info or from a different network."
            )

    if not wallets_to_fetch:
        if args.chain == "ethereum":
            raise SystemExit(
                "No analyzable Ethereum wallets found. Provide 0x-prefixed Ethereum wallet addresses "
                "or Ethereum transaction hashes that resolve to from/to wallet addresses."
            )
        raise SystemExit(
            "No analyzable Bitcoin wallets found. Provide Bitcoin wallet addresses "
            "or Bitcoin transaction hashes that resolve to wallet addresses."
        )

    graph, tx_by_hash = build_graph(wallets_to_fetch, seed_transactions, args.chain)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    features_df = extract_features(graph, wallets_to_fetch)
    related_df = build_related_wallets_dataframe(graph, wallets, max(1, int(args.related_depth)))
    c2_df = build_c2_signals_dataframe(tx_by_hash, args.chain, wallets)

    features_path = output_dir / "wallet_features.csv"
    related_path = output_dir / "related_wallets.csv"
    c2_signals_path = output_dir / "c2_signals.csv"
    graph_path = output_dir / "graph.graphml"

    features_df.to_csv(features_path, index=False)
    related_df.to_csv(related_path, index=False)
    c2_df.to_csv(c2_signals_path, index=False)
    nx.write_graphml(graph, graph_path)

    print("Analysis complete")
    print(f"Chain: {args.chain}")
    print(f"Wallets analyzed: {len(wallets_to_fetch)}")
    print(f"Unique transactions in graph: {len(tx_by_hash)}")
    print(f"Graph nodes: {graph.number_of_nodes()}")
    print(f"Graph edges: {graph.number_of_edges()}")
    print(f"Features CSV: {features_path}")
    print(f"Related wallets CSV: {related_path}")
    print(f"C2 signals CSV: {c2_signals_path}")
    print(f"GraphML: {graph_path}")
    print(f"Cache directory: {CACHE_DIR.resolve()}")


if __name__ == "__main__":
    main()

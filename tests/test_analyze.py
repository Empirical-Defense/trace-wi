import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import networkx as nx

import sys
from pathlib import Path
ROOT_PATH = str((Path(__file__).parent.parent).resolve())
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)
import src.analyze as analyze


class AnalyzePipelineTests(unittest.TestCase):
    def test_load_wallets_deduplicates_and_filters_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "wallets.json"
            wallet_a = "0x" + "a" * 40
            wallet_b = "0x" + "b" * 40
            payload = {
                "wallets": [wallet_a, wallet_b.upper(), wallet_a, "not-a-wallet"]
            }
            file_path.write_text(json.dumps(payload), encoding="utf-8")

            wallets = analyze.load_wallets(str(file_path), "ethereum")

            self.assertEqual(wallets, {wallet_a.lower(), wallet_b.lower()})

    def test_load_tx_hashes_accepts_prefixed_and_raw_hex(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "txs.json"
            raw = "a" * 64
            prefixed = "0x" + "b" * 64
            payload = {"transactions": [raw, prefixed, raw, "bad-tx"]}
            file_path.write_text(json.dumps(payload), encoding="utf-8")

            tx_hashes = analyze.load_tx_hashes(str(file_path), "ethereum")

            self.assertEqual(tx_hashes, {"0x" + raw, prefixed})

    def test_load_wallets_supports_bitcoin_addresses(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "wallets.json"
            payload = {
                "wallets": [
                    "1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
                    "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080",
                    "1G6EvUZcan2sBEE7omyYqFeEfcvEnZAZZP",
                    "not-a-wallet",
                ]
            }
            file_path.write_text(json.dumps(payload), encoding="utf-8")

            wallets = analyze.load_wallets(str(file_path), "bitcoin")

            self.assertEqual(
                wallets,
                {
                    "1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
                    "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080",
                },
            )

    def test_get_wallet_transactions_cache_hit_skips_api(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            analyze.CACHE_DIR = Path(tmpdir)
            wallet = "0x" + "1" * 40
            cache_path = analyze.get_wallet_cache_path(wallet, "ethereum")
            cache_payload = {
                "wallet": wallet,
                "transactions": [{"hash": "0x" + "f" * 64}],
                "tx_count": 1,
            }
            cache_path.write_text(json.dumps(cache_payload), encoding="utf-8")

            with patch("src.analyze.call_etherscan") as mock_api:
                txs = analyze.get_wallet_transactions(wallet, max_wallet_transactions=10_000, chain="ethereum")

            self.assertEqual(len(txs), 1)
            self.assertEqual(txs[0]["hash"], "0x" + "f" * 64)
            mock_api.assert_not_called()

    def test_get_wallet_transactions_cache_miss_paginates_and_caches(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            analyze.CACHE_DIR = Path(tmpdir)
            wallet = "0x" + "2" * 40

            tx_1 = {
                "hash": "0x" + "1" * 64,
                "from": wallet,
                "to": "0x" + "3" * 40,
                "value": "10",
                "timeStamp": "100",
            }
            tx_2 = {
                "hash": "0x" + "2" * 64,
                "from": wallet,
                "to": "0x" + "4" * 40,
                "value": "20",
                "timeStamp": "200",
            }

            responses = [
                {"status": "1", "message": "OK", "result": [tx_1]},
                {"status": "1", "message": "OK", "result": [tx_2]},
            ]

            def fake_call(params):
                if params["page"] == 1:
                    return responses[0]
                if params["page"] == 2:
                    return responses[1]
                return {"status": "1", "message": "OK", "result": []}

            with patch("src.analyze.ETH_DEFAULT_PAGE_SIZE", 1):
                with patch("src.analyze.call_etherscan", side_effect=fake_call) as mock_api:
                    txs = analyze.get_wallet_transactions(wallet, max_wallet_transactions=10_000, chain="ethereum")

            self.assertEqual(len(txs), 2)
            self.assertEqual(mock_api.call_count, 3)
            self.assertTrue(analyze.get_wallet_cache_path(wallet, "ethereum").exists())

    def test_get_wallet_transactions_stops_early_above_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            analyze.CACHE_DIR = Path(tmpdir)
            wallet = "0x" + "5" * 40

            txs_page = [
                {
                    "hash": "0x" + "a" * 63 + str(i),
                    "from": wallet,
                    "to": "0x" + "6" * 40,
                    "value": "1",
                    "timeStamp": "1",
                }
                for i in range(3)
            ]

            with patch("src.analyze.call_etherscan", return_value={"status": "1", "message": "OK", "result": txs_page}):
                result = analyze.get_wallet_transactions(wallet, max_wallet_transactions=2, chain="ethereum")

            self.assertGreater(len(result), 2)

    def test_extract_features_and_classification(self) -> None:
        wallet_a = "0x" + "a" * 40
        wallet_b = "0x" + "b" * 40

        graph = nx.DiGraph()
        graph.add_edge(wallet_a, wallet_b, value=5, timestamp=100)

        features = analyze.extract_features(graph, {wallet_a, wallet_b})
        rows = {row["wallet"]: row for row in features.to_dict(orient="records")}

        self.assertEqual(rows[wallet_a]["classification"], "distributor")
        self.assertEqual(rows[wallet_b]["classification"], "collector")
        self.assertEqual(rows[wallet_a]["transaction_count"], 1)
        self.assertEqual(rows[wallet_b]["transaction_count"], 1)

    def test_build_related_wallets_dataframe(self) -> None:
        seed = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"
        hop1 = "1P4NJHpyVUGa6W5PVr199dbHfj8WrPZrEv"
        hop2 = "1Prp6CayFNqVECivfGUsBGxi1pd4GqCes4"

        graph = nx.DiGraph()
        graph.add_edge(seed, hop1, tx_count=2, value=300)
        graph.add_edge(hop1, hop2, tx_count=1, value=50)

        df = analyze.build_related_wallets_dataframe(graph, {seed}, related_depth=2)
        rows = {(row["seed_wallet"], row["related_wallet"]): row for row in df.to_dict(orient="records")}

        self.assertIn((seed, hop1), rows)
        self.assertIn((seed, hop2), rows)
        self.assertEqual(rows[(seed, hop1)]["hop_distance"], 1)
        self.assertEqual(rows[(seed, hop1)]["relation_type"], "outgoing")
        self.assertEqual(rows[(seed, hop2)]["hop_distance"], 2)
        self.assertEqual(rows[(seed, hop2)]["relation_type"], "indirect")

    def test_build_c2_signals_dataframe(self) -> None:
        wallet = "1BkeGqpo8M5KNVYXW3obmQt1R58zXAqLBQ"
        tx_by_hash = {
            "tx_new": {
                "hash": "tx_new",
                "time": 200,
                "inputs": [{"prev_out": {"addr": "1A7wnTCAnoZFBu89PXZDBBNVAR2PVuMFqe"}}],
                "out": [{"addr": wallet, "value": 44367}],
            },
            "tx_prev": {
                "hash": "tx_prev",
                "time": 100,
                "inputs": [{"prev_out": {"addr": "1CeLgFDu917tgtunhJZ6BA2YdR559Boy9Y"}}],
                "out": [{"addr": wallet, "value": 258}],
            },
        }

        df = analyze.build_c2_signals_dataframe(tx_by_hash, "bitcoin", {wallet})
        row = df.to_dict(orient="records")[0]

        self.assertEqual(row["wallet"], wallet)
        self.assertEqual(row["latest_octet3"], 79)
        self.assertEqual(row["latest_octet4"], 173)
        self.assertEqual(row["previous_octet1"], 2)
        self.assertEqual(row["previous_octet2"], 1)
        self.assertEqual(row["candidate_ipv4"], "2.1.79.173")


if __name__ == "__main__":
    unittest.main()

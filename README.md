
# trace-wi: Blockchain Wallet Analysis Pipeline for Spycraft 2.1

This project provides a Python pipeline for analyzing blockchain wallet-transaction graphs, supporting both Bitcoin and Ethereum. It is designed for research, forensics, and data science use cases, with strict API rate limiting and local caching.

## Folder Structure

- `src/` — Main source code (Python modules)
	- `analyze.py` — Main pipeline script
	- `bitcoin_client.py` — Bitcoin API logic
	- `etherscan_client.py` — Ethereum API logic
- `data/` — Example input data
	- `wallets.json` — Example wallet addresses
	- `txs.json` — Example transaction hashes
- `tests/` — Unit tests
- `scripts/` — Utility scripts (e.g., for plotting)
- `wallet_features.csv`, `related_wallets.csv`, `c2_signals.csv`, `graph.graphml` — Example output files

## Quick Start

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Prepare input data

Place your wallet addresses and transaction hashes in the `data/` folder. Example files:

- `data/wallets.json`:
	```json
	{
		"wallets": [
			"15GqSWnxEFZezUCcGjhBMknA1PB7aYNXC1",
			"17gd1msp5FnMcEMF1MitTNSsYs7w7AQyCt"
			// ...
		]
	}
	```
- `data/txs.json`:
	```json
	{
		"transactions": [
			"1016d7ceff188e9fe32e68e9761bd811f354cfb31d7d106ec3c4f3ebce7f7a50",
			"14a0b3c26dc368d1b69862eb28fd8648fd218671bb052f62dfa093159ddd8e9a"
			// ...
		]
	}
	```

### 3. Run the pipeline

```bash
python src/analyze.py --chain bitcoin --wallets data/wallets.json --txs data/txs.json --output-dir .
```

For Ethereum:

```bash
export ETHERSCAN_API_KEY="YOUR_KEY"
python src/analyze.py --chain ethereum --wallets data/wallets.json --txs data/txs.json --output-dir .
```

#### Common flags
- `--chain`: `bitcoin` (default) or `ethereum`
- `--api-key`: override API key from environment
- `--max-wallet-txs`: change early-stop threshold (default `10000`)
- `--related-depth`: hop depth for related wallet chaining from seed wallets (default `1`)

### 4. Outputs

After running, you will find:

- `wallet_features.csv` — Wallet-level features (see file for columns)
- `related_wallets.csv` — Related wallet pairs and relationships
- `c2_signals.csv` — C2 signal events (if detected)
- `graph.graphml` — NetworkX-compatible graph of wallets/transactions

## Example Data Files

See the `data/` folder for sample input files. Output files are generated in the project root by default.

## Testing

Run all unit tests:

```bash
pytest tests/
```

## Notes

- Bitcoin API traffic is routed through `src/bitcoin_client.py`.
- Ethereum API traffic is routed through `src/etherscan_client.py`.
- Caching is automatic; repeated runs skip already-fetched wallets.

import math
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output"


def short_wallet(wallet: str, start: int = 8, end: int = 6) -> str:
    if not isinstance(wallet, str):
        return ""
    if len(wallet) <= start + end + 1:
        return wallet
    return f"{wallet[:start]}...{wallet[-end:]}"


def build_hub_concentration_curve(wallet_features: pd.DataFrame) -> None:
    ordered = wallet_features.sort_values("transaction_count", ascending=False).reset_index(drop=True)
    ordered["rank"] = ordered.index + 1
    ordered["cum_tx"] = ordered["transaction_count"].cumsum()
    total_tx = max(float(ordered["transaction_count"].sum()), 1.0)
    ordered["cum_share"] = ordered["cum_tx"] / total_tx

    plt.figure(figsize=(10, 6), dpi=180)
    plt.plot(ordered["rank"], ordered["cum_share"], color="#0f4c81", linewidth=2.0)
    plt.scatter(ordered["rank"], ordered["cum_share"], color="#0f4c81", s=10)
    plt.axhline(0.5, color="#888888", linestyle="--", linewidth=0.9)
    plt.axhline(0.8, color="#aaaaaa", linestyle=":", linewidth=0.9)
    plt.title("Hub Concentration Curve", fontsize=13)
    plt.xlabel("Wallet rank by transaction_count")
    plt.ylabel("Cumulative share of interactions")
    plt.ylim(0, 1.02)
    plt.xlim(1, max(1, len(ordered)))
    plt.grid(alpha=0.2)
    plt.tight_layout()
    plt.savefig(OUT / "hub_concentration_curve.png", bbox_inches="tight", pad_inches=0.05)
    plt.close()


def build_decode_frequency_table(c2_signals: pd.DataFrame) -> None:
    valid = c2_signals[
        (c2_signals["candidate_ipv4"].notna()) & (c2_signals["signal_event_count"] > 0)
    ].copy()
    valid = valid.sort_values("signal_event_count", ascending=False).head(12)

    if valid.empty:
        valid = pd.DataFrame(
            [{"wallet": "N/A", "candidate_ipv4": "N/A", "signal_event_count": 0}]
        )

    table_df = valid[["wallet", "candidate_ipv4", "signal_event_count"]].copy()
    table_df["wallet"] = table_df["wallet"].map(short_wallet)
    table_df = table_df.rename(
        columns={
            "wallet": "Wallet",
            "candidate_ipv4": "Candidate IPv4",
            "signal_event_count": "Signal events",
        }
    )

    fig_height = 1.4 + 0.42 * len(table_df)
    fig, ax = plt.subplots(figsize=(9.5, fig_height), dpi=200)
    ax.axis("off")
    ax.set_title("Decode Frequency Table (Top wallets)", fontsize=13, pad=10)

    table = ax.table(
        cellText=table_df.values,
        colLabels=table_df.columns,
        cellLoc="left",
        colLoc="left",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.25)

    for (row, col), cell in table.get_celld().items():
        if row == 0:
            cell.set_text_props(weight="bold", color="#ffffff")
            cell.set_facecolor("#0f4c81")
        else:
            cell.set_facecolor("#f8fbff" if row % 2 == 0 else "#ffffff")

    plt.tight_layout()
    plt.savefig(OUT / "decode_frequency_table.png", bbox_inches="tight", pad_inches=0.05)
    plt.close()


def build_role_split_chart(wallet_features: pd.DataFrame) -> None:
    counts = wallet_features["classification"].value_counts()
    order = ["transit", "collector", "distributor", "isolated"]
    series = pd.Series({k: int(counts.get(k, 0)) for k in order})

    plt.figure(figsize=(8, 5), dpi=180)
    bars = plt.bar(series.index, series.values, color=["#d95f02", "#1b9e77", "#7570b3", "#666666"])
    plt.title("Wallet Role Split", fontsize=13)
    plt.ylabel("Wallet count")
    plt.grid(axis="y", alpha=0.2)

    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            height + 0.2,
            f"{int(height)}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    plt.tight_layout()
    plt.savefig(OUT / "role_split_chart.png", bbox_inches="tight", pad_inches=0.05)
    plt.close()


def build_seed_neighborhood_map(related_wallets: pd.DataFrame) -> None:
    rw = related_wallets.copy()
    rw["direct_tx_count"] = pd.to_numeric(rw["direct_tx_count"], errors="coerce").fillna(0)
    rw["hop_distance"] = pd.to_numeric(rw["hop_distance"], errors="coerce").fillna(1).astype(int)

    seeds = sorted(rw["seed_wallet"].dropna().unique().tolist())
    max_related = 180
    related_rank = (
        rw.groupby("related_wallet", as_index=False)["direct_tx_count"]
        .sum()
        .sort_values("direct_tx_count", ascending=False)
    )
    keep_related = set(related_rank.head(max_related)["related_wallet"].tolist())
    rw = rw[rw["related_wallet"].isin(keep_related)]

    g = nx.Graph()
    for seed in seeds:
        g.add_node(seed, node_type="seed", hop=0)

    for _, row in rw.iterrows():
        seed = row["seed_wallet"]
        related = row["related_wallet"]
        hop = int(row["hop_distance"])
        weight = float(row.get("direct_tx_count", 1.0) or 1.0)
        if pd.isna(seed) or pd.isna(related):
            continue
        g.add_node(related, node_type="related", hop=hop)
        g.add_edge(seed, related, weight=max(0.3, min(4.0, math.log10(weight + 1) + 0.4)))

    if g.number_of_nodes() == 0:
        return

    pos = nx.spring_layout(g, seed=42, k=0.7)

    seed_nodes = [n for n, d in g.nodes(data=True) if d.get("node_type") == "seed"]
    related_nodes = [n for n in g.nodes if n not in seed_nodes]

    plt.figure(figsize=(12, 9), dpi=200)
    nx.draw_networkx_edges(
        g,
        pos,
        alpha=0.25,
        width=[g[u][v].get("weight", 0.4) for u, v in g.edges()],
        edge_color="#999999",
    )
    nx.draw_networkx_nodes(
        g,
        pos,
        nodelist=related_nodes,
        node_size=28,
        node_color="#4ea1d3",
        alpha=0.85,
    )
    nx.draw_networkx_nodes(
        g,
        pos,
        nodelist=seed_nodes,
        node_size=120,
        node_color="#ef6c00",
        alpha=0.95,
    )

    label_seeds = {n: short_wallet(n, 6, 4) for n in seed_nodes[:14]}
    nx.draw_networkx_labels(g, pos, labels=label_seeds, font_size=7)

    plt.title("Seed-to-Neighborhood Expansion Map", fontsize=13)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(OUT / "seed_neighborhood_map.png", bbox_inches="tight", pad_inches=0.05)
    plt.close()


def build_recurrence_timeline(c2_signals: pd.DataFrame) -> None:
    cs = c2_signals.copy()
    cs = cs[(cs["candidate_ipv4"].notna()) & (cs["signal_event_count"] > 0)]

    if cs.empty:
        return

    cs["latest_seen_dt"] = pd.to_datetime(cs["latest_seen"], errors="coerce", utc=True)
    cs["previous_seen_dt"] = pd.to_datetime(cs["previous_seen"], errors="coerce", utc=True)
    cs = cs.dropna(subset=["latest_seen_dt"]).sort_values("latest_seen_dt")

    plt.figure(figsize=(11, 6), dpi=200)

    for _, row in cs.iterrows():
        if pd.notna(row["previous_seen_dt"]):
            plt.plot(
                [row["previous_seen_dt"], row["latest_seen_dt"]],
                [row["wallet"], row["wallet"]],
                color="#b0b0b0",
                alpha=0.45,
                linewidth=1.0,
            )

    sizes = cs["signal_event_count"].clip(lower=1).pow(0.6) * 12
    plt.scatter(
        cs["latest_seen_dt"],
        cs["wallet"],
        s=sizes,
        color="#c62828",
        alpha=0.85,
        label="Latest decoded recurrence",
    )

    top = cs.sort_values("signal_event_count", ascending=False).head(6)
    for _, row in top.iterrows():
        plt.text(
            row["latest_seen_dt"],
            row["wallet"],
            f" {short_wallet(row['wallet'], 6, 4)} ({int(row['signal_event_count'])})",
            fontsize=7,
            va="center",
        )

    plt.title("Decoded Candidate Recurrence Timeline", fontsize=13)
    plt.xlabel("Last observed decoded event time")
    plt.ylabel("Wallet")
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m", tz=mdates.UTC))
    plt.grid(alpha=0.2)
    plt.tight_layout()
    plt.savefig(OUT / "decoded_recurrence_timeline.png", bbox_inches="tight", pad_inches=0.05)
    plt.close()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    wallet_features = pd.read_csv(ROOT / "wallet_features.csv")
    related_wallets = pd.read_csv(ROOT / "related_wallets.csv")
    c2_signals = pd.read_csv(ROOT / "c2_signals.csv")

    build_hub_concentration_curve(wallet_features)
    build_decode_frequency_table(c2_signals)
    build_role_split_chart(wallet_features)
    build_seed_neighborhood_map(related_wallets)
    build_recurrence_timeline(c2_signals)

    print("Wrote output/hub_concentration_curve.png")
    print("Wrote output/decode_frequency_table.png")
    print("Wrote output/role_split_chart.png")
    print("Wrote output/seed_neighborhood_map.png")
    print("Wrote output/decoded_recurrence_timeline.png")


if __name__ == "__main__":
    main()

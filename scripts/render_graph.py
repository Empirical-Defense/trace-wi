import matplotlib.pyplot as plt
import networkx as nx


def main() -> None:
    g = nx.read_graphml("graph.graphml")

    # Full graph image (dense overview)
    plt.figure(figsize=(18, 18), dpi=220)
    pos = nx.spring_layout(g, seed=42, k=0.15)
    nx.draw_networkx_nodes(g, pos, node_size=5, alpha=0.7, node_color="#1f77b4")
    nx.draw_networkx_edges(g, pos, width=0.1, alpha=0.2, edge_color="#999999")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig("output/graph_full.png", bbox_inches="tight", pad_inches=0.02)
    plt.close()

    # Hub-focused subgraph for readability
    by_degree = sorted(g.degree, key=lambda x: x[1], reverse=True)
    top_nodes = [n for n, _ in by_degree[:120]]
    h = g.subgraph(top_nodes).copy()

    plt.figure(figsize=(16, 16), dpi=240)
    pos_h = nx.spring_layout(h, seed=42, k=0.35)
    degs = dict(h.degree())
    node_sizes = [30 + 12 * degs[n] for n in h.nodes()]
    nx.draw_networkx_nodes(h, pos_h, node_size=node_sizes, alpha=0.9, node_color="#e4572e")
    nx.draw_networkx_edges(h, pos_h, width=0.4, alpha=0.35, edge_color="#4a4a4a")

    hub_labels = {n: n for n, d in sorted(h.degree, key=lambda x: x[1], reverse=True)[:15]}
    nx.draw_networkx_labels(h, pos_h, labels=hub_labels, font_size=6)

    plt.axis("off")
    plt.tight_layout()
    plt.savefig("output/graph_hubs.png", bbox_inches="tight", pad_inches=0.02)
    plt.close()

    print("Wrote output/graph_full.png")
    print("Wrote output/graph_hubs.png")


if __name__ == "__main__":
    main()

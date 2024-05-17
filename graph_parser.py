import json

COLOR_TO_PLATFORM = {
    "lightyellow": "universal",
    "aqua": "mac",
    "lightcoral": "linux",
}


class Node:
    def __init__(self, node, edges):
        self.id = node["_gvid"]
        self.name = node["name"]
        self.platform = COLOR_TO_PLATFORM[node["fillcolor"]]
        self.depends_on = self.get_edges(edges)

    def get_edges(self, edges: list[dict]) -> list[str]:
        depends_on = []
        for edge in edges:
            if edge["head"] == self.id:
                depends_on.append(edge["tail"])

        return depends_on


class Graph:
    def __init__(self, graph_file):
        self.nodes = self.parse(graph_file)

    def parse(self, graph_file: str) -> list[Node]:
        graph = json.load(open(graph_file))
        nodes_raw = graph["objects"]
        edges = graph["edges"]

        return [Node(node, edges) for node in nodes_raw]


if __name__ == "__main__":
    graph = Graph("graph.json")
    for node in graph.nodes:
        print(f"Node: {node.name} depends on {node.depends_on}")

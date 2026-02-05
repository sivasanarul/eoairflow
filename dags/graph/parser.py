import json
from dags.graph.graph import Graph
from dags.graph.node import Node

def load_graph_from_json(path):
    with open(path) as f:
        data = json.load(f)

    graph = Graph()

    for n in data["nodes"]:
        graph.add_node(
            Node(
                node_id=n["id"],
                type=n["type"],
                image=n.get("image"),
                command=n.get("command"),
                query=n.get("query"),
            )
        )

    for upstream, downstream in data["edges"]:
        graph.add_edge(upstream, downstream)

    return data, graph

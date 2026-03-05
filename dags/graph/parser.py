import json
from graph.graph import Graph
from graph.node import Node

def load_graph_from_json(path):
    with open(path) as f:
        data = json.load(f)

    graph = Graph()
    
    # Get common volumes from JSON
    common_volumes = data.get("common_volumes", [])

    for n in data["nodes"]:
        # Combine common volumes with node-specific volumes
        node_volumes = common_volumes + n.get("volumes", [])
        
        graph.add_node(
            Node(
                node_id=n["id"],
                type=n["type"],
                image=n.get("image"),
                command=n.get("command"),
                query=n.get("query"),
                environment=n.get("environment", {}),
                volumes=node_volumes,
                network_mode=n.get("network_mode"),
            )
        )

    for upstream, downstream in data.get("edges", []):
        graph.add_edge(upstream, downstream)

    return data, graph

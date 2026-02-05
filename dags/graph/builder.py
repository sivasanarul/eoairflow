from dags.operators.docker import docker_operator
from dags.operators.db_sensor import db_sensor_operator

def build_dag(graph, dag, db_config):
    task_map = {}

    # Create Airflow tasks
    for node in graph.nodes.values():
        if node.type == "docker":
            task_map[node.node_id] = docker_operator(
                task_id=node.node_id,
                image=node.image,
                command=node.command,
            )

        elif node.type == "db_sensor":
            task_map[node.node_id] = db_sensor_operator(
                task_id=node.node_id,
                query=node.query,
                db_config=db_config,
            )

        else:
            raise ValueError(f"Unknown node type: {node.type}")

    # Wire dependencies
    for upstream, downstream in graph.edges:
        task_map[upstream] >> task_map[downstream]

    return task_map

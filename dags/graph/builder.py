from operators.docker import docker_operator
from operators.db_sensor import db_sensor_operator


def build_dag(graph, dag, db_config, global_env=None, global_volumes=None):
    """Build Airflow DAG from a Graph definition.
    
    Args:
        graph: Graph object containing nodes and edges
        dag: Airflow DAG instance
        db_config: Database configuration for db_sensor nodes
        global_env: Environment variables applied to all Docker nodes
        global_volumes: Volume mounts applied to all Docker nodes
    """
    task_map = {}
    global_env = global_env or {}
    global_volumes = global_volumes or []

    # Create Airflow tasks
    for node in graph.nodes.values():
        if node.type == "docker":
            # Merge global and node-specific environment variables
            merged_env = {**global_env, **node.environment}
            # Combine global and node-specific volumes
            merged_volumes = global_volumes + node.volumes
            
            task_map[node.node_id] = docker_operator(
                task_id=node.node_id,
                image=node.image,
                command=node.command,
                environment=merged_env,
                volumes=merged_volumes,
                network_mode=node.network_mode,
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

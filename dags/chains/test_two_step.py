from airflow import DAG
from datetime import datetime
from pathlib import Path

from graph.parser import load_graph_from_json
from graph.builder import build_dag
from common.defaults import default_args, DB_CONFIG

CHAIN_FILE = Path(__file__).parents[2] / "config" / "chains" / "test_two_step.json"

data, graph = load_graph_from_json(str(CHAIN_FILE))

with DAG(
    dag_id=data["dag_id"],
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:
    build_dag(graph, dag, DB_CONFIG)

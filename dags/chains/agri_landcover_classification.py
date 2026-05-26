"""
Landcover Classification Chain DAG - Crop LC Classification Workflow

This DAG implements a landcover classification workflow loaded from JSON configuration.
Runs crop landcover classification training and prediction workflow.

Runtime parameters can be configured when triggering the DAG.
"""

from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from pathlib import Path

from graph.parser import load_graph_from_json
from graph.builder import build_dag
from common.defaults import default_args, DB_CONFIG


# Load configuration from JSON
CHAIN_FILE = Path(__file__).parents[2] / "config" / "chains" / "agri_landcover_classification_chain.json"
data, graph = load_graph_from_json(str(CHAIN_FILE))

# Build DAG parameters from JSON params with Param objects for UI
json_params = data.get("params", {})
dag_params = {
    "project": Param(json_params.get("project", "AIRFLOW"), type="string", description="Project name"),
    "environment": Param(json_params.get("environment", "TEST"), type="string", description="Environment"),
    "service_name": Param(json_params.get("service_name", "PREFIX-LC"), type="string", description="Service/analysis type"),
    "analysis_time": Param(json_params.get("analysis_time", "20250731"), type="string", description="Analysis yearmonth (YYYYMMDD)"),
    "traindataset": Param(json_params.get("traindataset", "no"), type="string", description="Use training dataset (true/false)"),
    "traindataset_parcelcolumn": Param(json_params.get("traindataset_parcelcolumn", None), type=["null", "string"], description="Training dataset parcel column name"),
    "remote_name": Param(json_params.get("remote_name", ""), type="string", description="Remote name for input data"),
    "foi": Param(json_params.get("foi", "no"), type="string", description="Feature of Interest flag (yes/no)"),
    "subtiling": Param(json_params.get("subtiling", "yes"), type="string", description="Subtiling flag (yes/no)"),
    "input_archive_roots": Param(json_params.get("input_archive_roots", "/mnt/hddarchive.nfs/output"), type="string", description="Input archive paths"),
    "output_archive_root": Param(json_params.get("output_archive_root", "/mnt/hddarchive.nfs/output"), type="string", description="Output archive root"),
    "output_archive_tmp": Param(json_params.get("output_archive_tmp", "/mnt/hddarchive.nfs/output.tmp"), type="string", description="Temp output path"),
    "support_data": Param(json_params.get("support_data", "/mnt/ssdarchive.nfs/support_data"), type="string", description="Support data path"),
    "worker_group": Param(json_params.get("worker_group", "classification"), type="string", description="Worker group"),
    "run_no": Param(json_params.get("run_no", "RUN1"), type="string", description="Run number"),
}

with DAG(
    dag_id=data.get("dag_id", "agri_landcover_classification"),
    start_date=datetime(2024, 1, 1),
    schedule_interval=data.get("schedule"),
    catchup=False,
    max_active_runs=data.get("max_active_runs", 1),
    default_args=default_args,
    params=dag_params,
) as dag:
    build_dag(graph, dag, DB_CONFIG)

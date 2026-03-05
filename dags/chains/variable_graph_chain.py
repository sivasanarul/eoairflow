"""
Variable Graph Chain DAG - Anomaly Processing Workflow

This DAG implements an anomaly detection workflow:
1. Two Docker containers run in parallel:
   - ndvi_zscore: NDVI LPIS zscore calculation (tondor/tool/baresoil/ndvi_zscore.py)
   - ndvimax_zscore: NDVI Max zscore calculation (tondor/tool/baresoil/ndvimax_zscore.py)
2. submit_anomaly: Submits anomaly tasks to workers (cop4n2k/src/gisat-internal/submit_anomaly.py)
3. wait_db: Polls until all anomaly tasks have status 'K' (completed)
4. merge_anomaly: Merges results (cop4n2k/src/merge_scripts/merge_anomaly.py)

Runtime parameters can be configured when triggering the DAG.
"""

import json
from airflow import DAG
from airflow.models.param import Param
from datetime import datetime

from graph.graph import Graph
from graph.node import Node
from graph.builder import build_dag
from common.defaults import default_args, DB_CONFIG


# DAG parameters for runtime configuration
dag_params = {
    # Common parameters
    "project": Param("SKSAMAS", type="string", description="Project name (e.g., SKSAMAS, SAMAS)"),
    "environment": Param("PROD", type="string", enum=["PROD", "DEV"], description="Environment"),
    "service": Param("HM", type="string", description="Service name (e.g., HM, TTP)"),
    "pixel_size": Param("10", type="string", enum=["10", "20"], description="Output pixel size in metres"),
    
    # Time parameters
    "yearmonth": Param("20250630", type="string", description="Analysis yearmonth (e.g., 20250630)"),
    "analysis_time": Param("20230701-20240630", type="string", description="Analysis time range for anomaly"),
    "yearfrom": Param("202401", type="string", description="Start yearmonth for ndvimax_zscore"),
    "yearto": Param("202412", type="string", description="End yearmonth for ndvimax_zscore"),
    
    # Archive paths
    "input_archive_roots": Param(
        "/mnt/hddarchive.nfs/output",
        type="string",
        description="Colon-separated input archive paths"
    ),
    "output_archive_root": Param(
        "/mnt/hddarchive.nfs/output",
        type="string",
        description="Output archive root path"
    ),
    "output_archive_tmp": Param(
        "/mnt/hddarchive.nfs/output.tmp",
        type="string",
        description="Temporary output archive path"
    ),
    "support_data": Param(
        "/mnt/ssdarchive.nfs/support_data",
        type="string",
        description="Support data folder path"
    ),
    
    # Worker configuration
    "worker_group": Param("ndvi", type="string", description="Worker group for task submission"),
    "run_no": Param("RUN1", type="string", description="Run number identifier"),
}

with DAG(
    dag_id="anomaly_processing_chain",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params=dag_params,
    render_template_as_native_obj=True,
) as dag:

    g = Graph()

    # Common volume mounts for all containers
    common_volumes = [
        "/mnt/hddarchive.nfs:/mnt/hddarchive.nfs",
        "/mnt/ssdarchive.nfs:/mnt/ssdarchive.nfs",
        "/support_data:/support_data:ro",
    ]

    # Build TONDOR_KWARGS JSON for ndvi_zscore
    # Uses Jinja templating to access DAG params at runtime
    ndvi_zscore_kwargs = json.dumps({
        "yearmonth": "{{ params.yearmonth }}",
        "pixel_size": "{{ params.pixel_size }}",
        "project": "{{ params.project }}",
        "service": "{{ params.service }}",
    })

    # Task A: NDVI LPIS zscore calculation
    # Source: tondor/src/tondor/tool/baresoil/ndvi_zscore.py
    g.add_node(Node(
        node_id="ndvi_zscore",
        type="docker",
        image="optcomposite:latest",
        command="python3 -m tondor.tool.baresoil.ndvi_zscore",
        environment={
            "TONDOR_TASK_ID": "{{ task_instance.task_id }}",
            "TONDOR_KWARGS": ndvi_zscore_kwargs,
            "INPUT_ARCHIVE_ROOTS": "{{ params.input_archive_roots }}",
            "OUTPUT_ARCHIVE_ROOT": "{{ params.output_archive_root }}",
            "OUTPUT_ARCHIVE_TMP": "{{ params.output_archive_tmp }}",
        },
        volumes=common_volumes,
        network_mode="host",
    ))

    # Build environment for ndvimax_zscore
    ndvimax_zscore_kwargs = json.dumps({
        "yearfrom": "{{ params.yearfrom }}",
        "yearto": "{{ params.yearto }}",
        "yearmonth": "{{ params.yearmonth }}",
        "pixel_size": "{{ params.pixel_size }}",
        "project": "{{ params.project }}",
        "environment": "{{ params.environment }}",
        "service": "{{ params.service }}",
    })

    # Task B: NDVI Max zscore calculation
    # Source: tondor/src/tondor/tool/baresoil/ndvimax_zscore.py
    g.add_node(Node(
        node_id="ndvimax_zscore",
        type="docker",
        image="optcomposite:latest",
        command="python3 -m tondor.tool.baresoil.ndvimax_zscore",
        environment={
            "TONDOR_TASK_ID": "{{ task_instance.task_id }}",
            "TONDOR_KWARGS": ndvimax_zscore_kwargs,
            "INPUT_ARCHIVE_ROOTS": "{{ params.input_archive_roots }}",
            "OUTPUT_ARCHIVE_ROOT": "{{ params.output_archive_root }}",
            "OUTPUT_ARCHIVE_TMP": "{{ params.output_archive_tmp }}",
        },
        volumes=common_volumes,
        network_mode="host",
    ))

    # Submit anomaly tasks - runs after zscore calculations complete
    # Source: cop4n2k/src/gisat-internal/submit_anomaly.py
    g.add_node(Node(
        node_id="submit_anomaly",
        type="docker",
        image="cop4n2k:latest",
        command="python3 /app/src/gisat-internal/submit_anomaly.py",
        environment={
            "ANALYSIS_TIME": "{{ params.analysis_time }}",
            "PROJECT": "{{ params.project }}",
            "ENVIRONMENT": "{{ params.environment }}",
            "SERVICE_NAME": "{{ params.service }}",
            "PIXEL_SIZE": "{{ params.pixel_size }}",
            "WORKER_GROUP": "{{ params.worker_group }}",
            "RUN_NO": "{{ params.run_no }}",
        },
        volumes=common_volumes,
        network_mode="host",
    ))

    # Database sensor - polls until all anomaly tasks have status 'K'
    # Query uses Jinja templating to match the specific job pattern
    g.add_node(Node(
        node_id="wait_db",
        type="db_sensor",
        query="""
            SELECT COUNT(*)
            FROM supervisor_job AS job
            INNER JOIN supervisor_jobstep AS job_step ON job.id = job_step.job_id
            INNER JOIN supervisor_task AS task ON job_step.id = task.job_step_id
            WHERE job.name LIKE '%ANOMALY%'
              AND (task.status IS NULL OR task.status != 'K');
        """,
    ))

    # Merge anomaly results - runs after all tasks are complete
    # Source: cop4n2k/src/merge_scripts/merge_anomaly.py
    g.add_node(Node(
        node_id="merge_anomaly",
        type="docker",
        image="cop4n2k:latest",
        command="python3 /app/src/merge_scripts/merge_anomaly.py",
        environment={
            "PROJECT": "{{ params.project }}",
            "ENVIRONMENT": "{{ params.environment }}",
            "TIMEPERIOD": "{{ params.analysis_time }}",
            "SERVICE": "{{ params.service }}",
            "OUTPUT_ARCHIVE_ROOT": "{{ params.output_archive_root }}",
        },
        volumes=common_volumes,
        network_mode="host",
    ))

    # Define edges - creates the dependency graph:
    # ndvi_zscore ────┐
    #                 ├──> submit_anomaly ──> wait_db ──> merge_anomaly
    # ndvimax_zscore ─┘
    g.add_edge("ndvi_zscore", "submit_anomaly")
    g.add_edge("ndvimax_zscore", "submit_anomaly")
    g.add_edge("submit_anomaly", "wait_db")
    g.add_edge("wait_db", "merge_anomaly")

    build_dag(g, dag, DB_CONFIG)

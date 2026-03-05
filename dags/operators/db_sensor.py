from airflow.sensors.python import PythonSensor
from common.db import condition_met

def db_sensor_operator(*, task_id, query, db_config):
    return PythonSensor(
        task_id=task_id,
        python_callable=condition_met,
        op_kwargs={
            "query": query,
            "db_config": db_config,
        },
        poke_interval=60,
        timeout=6 * 60 * 60,
        mode="reschedule",
    )

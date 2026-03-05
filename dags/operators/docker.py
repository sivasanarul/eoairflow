from airflow.providers.docker.operators.docker import DockerOperator
from typing import Dict, List, Optional


def docker_operator(
    *,
    task_id: str,
    image: str,
    command: str,
    environment: Optional[Dict[str, str]] = None,
    volumes: Optional[List[str]] = None,
    network_mode: Optional[str] = None,
):
    """Create a DockerOperator with configurable environment and volumes.
    
    Args:
        task_id: Unique task identifier
        image: Docker image to run
        command: Command to execute in the container
        environment: Dict of environment variables to pass to the container
        volumes: List of volume mounts in "host:container" format
        network_mode: Docker network mode (e.g., "bridge", "host")
    """
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment=environment or {},
        mounts=[],  # Use volumes instead for simpler syntax
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        tty=True,
        network_mode=network_mode,
        mount_tmp_dir=False,
    )

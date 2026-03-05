from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from typing import Dict, List, Optional


def parse_volume_string(volume_str: str) -> Mount:
    """Parse a volume string like '/host/path:/container/path:ro' into a Mount object."""
    parts = volume_str.split(":")
    if len(parts) == 2:
        source, target = parts
        read_only = False
    elif len(parts) == 3:
        source, target, mode = parts
        read_only = mode == "ro"
    else:
        raise ValueError(f"Invalid volume format: {volume_str}")
    
    return Mount(target=target, source=source, type="bind", read_only=read_only)


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
        volumes: List of volume mounts in "host:container[:ro]" format
        network_mode: Docker network mode (e.g., "bridge", "host")
    """
    # Convert volume strings to Mount objects
    mounts = [parse_volume_string(v) for v in (volumes or [])]
    
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment=environment or {},
        mounts=mounts,
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        tty=True,
        network_mode=network_mode,
        mount_tmp_dir=False,
    )

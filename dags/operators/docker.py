from airflow.providers.docker.operators.docker import DockerOperator

def docker_operator(*, task_id, image, command):
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        tty=True,
    )

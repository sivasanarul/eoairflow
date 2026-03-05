from dataclasses import dataclass, field
from typing import Optional, Literal, Dict, List

NodeType = Literal["docker", "db_sensor"]

@dataclass
class Node:
    node_id: str
    type: NodeType
    image: Optional[str] = None
    command: Optional[str] = None
    query: Optional[str] = None
    # Environment variables for Docker containers
    environment: Dict[str, str] = field(default_factory=dict)
    # Volume mounts for Docker containers (list of "host:container" strings)
    volumes: List[str] = field(default_factory=list)
    # Docker network configuration
    network_mode: Optional[str] = None

from dataclasses import dataclass
from typing import Optional, Literal

NodeType = Literal["docker", "db_sensor"]

@dataclass
class Node:
    node_id: str
    type: NodeType
    image: Optional[str] = None
    command: Optional[str] = None
    query: Optional[str] = None

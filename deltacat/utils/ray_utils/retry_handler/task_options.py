from dataclasses import dataclass
from typing import Any, Optional

@dataclass
class RayRemoteTaskOptions():
    """
    Represents the options corresponding to Ray remote task
    """
    def __init__(self,
                 memory: Optional[float] = None,
                 num_cpus: Optional[int] = None,
                 placement_group: Optional[Any] = None) -> None:
        self.memory = memory
        self.num_cpus = num_cpus
        self.placement_group = placement_group
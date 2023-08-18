from typing import Any, Optional, Dict


class RayRemoteTaskOptions(Dict):
    """
    Represents the options corresponding to Ray remote task
    """

    def __init__(self,
                 memory: Optional[float] = None,
                 num_cpus: Optional[int] = None,
                 placement_group: Optional[Any] = None,
                 scheduling_strategy: Optional[Any] = None) -> None:
        self["memory"] = memory
        self["num_cpus"] = num_cpus
        self["placement_group"] = placement_group
        self["scheduling_strategy"] = scheduling_strategy

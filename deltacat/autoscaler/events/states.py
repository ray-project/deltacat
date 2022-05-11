from enum import Enum, auto

from ray.autoscaler._private.event_system import EventSequence, CreateClusterEvent, StateEvent


class States(Enum):
    UNKNOWN = None
    NEW = 1
    DISPATCHED = 2
    STARTED = 3
    IN_PROGRESS = 4
    COMPLETED = 5


class ScriptStartedEvent(StateEvent):
    """Events to track for Ray scripts that are executed.
    """
    @property
    def state(self) -> str:
        return States.STARTED.name

    start_initializing = auto()


class ScriptInProgressEvent(StateEvent):
    """Events tracking Ray app execution progress.
    """
    @property
    def state(self) -> str:
        return States.IN_PROGRESS.name

    in_progress = auto()


class ScriptInProgressCustomEvent(EventSequence):
    """Custom, user-defined events to track during execution of Ray scripts.
    """
    def __init__(self, event_name: str, state_sequence: int):
        self.event_name = event_name
        self.state_sequence = state_sequence

    @property
    def state(self) -> str:
        return States.IN_PROGRESS.name

    @property
    def name(self) -> str:
        return self.event_name

    @property
    def value(self) -> int:
        # the state sequence number in 1-based indexing
        return self.state_sequence + 1


class ScriptCompletedEvent(StateEvent):
    """Event marking the start of Ray app execution.
    """
    @property
    def state(self) -> str:
        return States.COMPLETED.name

    completed = auto()


stats_metadata_collection_started_event = ScriptInProgressCustomEvent("STATS_METADATA_COLLECTION_STARTED", 1)
stats_metadata_collection_completed_event = ScriptInProgressCustomEvent("STATS_METADATA_COLLECTION_COMPLETED", 2)
compaction_session_started_event = ScriptInProgressCustomEvent("COMPACTION_SESSION_STARTED", 3)
compaction_session_completed_event = ScriptInProgressCustomEvent("COMPACTION_SESSION_COMPLETED", 4)
custom_events = [stats_metadata_collection_started_event,
                 stats_metadata_collection_completed_event,
                 compaction_session_started_event,
                 compaction_session_completed_event]

event_enums = [CreateClusterEvent, ScriptStartedEvent, ScriptInProgressEvent, ScriptCompletedEvent]
event_enum_values = [sequence for event in event_enums
                     for sequence in event.__members__.values()]

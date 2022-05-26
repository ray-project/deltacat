import json
import logging
from typing import List, Union, Dict, Set

from deltacat import logs
from deltacat.autoscaler.events.event_store import EventStoreClient
from deltacat.autoscaler.events.workflow import EventWorkflow, StateTransitionMap
from deltacat.autoscaler.events.compaction.dispatcher import CompactionEventDispatcher
from deltacat.autoscaler.events.states import ScriptStartedEvent, ScriptInProgressEvent, \
    ScriptCompletedEvent, States, ScriptInProgressCustomEvent

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


STATS_METADATA_COLLECTION_STARTED = "STATS_METADATA_COLLECTION_STARTED"
STATS_METADATA_COLLECTION_COMPLETED = "STATS_METADATA_COLLECTION_COMPLETED"
COMPACTION_SESSION_STARTED = "COMPACTION_SESSION_STARTED"
COMPACTION_SESSION_PARTITION_COMPLETED = "COMPACTION_SESSION_PARTITION_COMPLETED"
COMPACTION_SESSION_COMPLETED = "COMPACTION_SESSION_COMPLETED"


class CompactionWorkflow(EventWorkflow):

    def __init__(self,
                 event_dispatcher: CompactionEventDispatcher,
                 event_store: EventStoreClient = None,
                 partition_ids_to_compact: Set[str] = None):
        self.event_dispatcher = event_dispatcher
        assert self.event_dispatcher is not None, f"Event dispatcher must be provided to build and transition " \
                                                  f"to different job states."
        self.event_store = event_store
        self.partition_ids_to_compact = partition_ids_to_compact

        # Initialization
        self._build_states()
        self.state_transitions = self._build_state_transitions()
        self.start_initializing()

    @property
    def state_transition_map(self) -> StateTransitionMap:
        return self.state_transitions

    def _build_states(self):
        events = [
            ScriptInProgressCustomEvent(STATS_METADATA_COLLECTION_STARTED, 1),
            ScriptInProgressCustomEvent(STATS_METADATA_COLLECTION_COMPLETED, 2),
            ScriptInProgressCustomEvent(COMPACTION_SESSION_STARTED, 3),
            ScriptInProgressCustomEvent(COMPACTION_SESSION_PARTITION_COMPLETED, 4),
            ScriptInProgressCustomEvent(COMPACTION_SESSION_COMPLETED, 5)
        ]
        self._event_map = event_map = {event.name: event for event in events}
        self.event_dispatcher.add_event_handlers([val for _, val in event_map.items()])

    def _build_state_transitions(self) -> StateTransitionMap:
        """Builds a mapping of event states to state transitioning callbacks, or
        a dictionary of state transitioning callbacks.

        If an event has state sequences, a dictionary of callbacks is provided
        with sequences as keys and callback functions as values.

        Returns: a map of event states to callbacks or a dictionary of callbacks
        """
        init_sequence = 0
        states = self._event_map
        return {
            States.STARTED.name: self.in_progress,
            States.IN_PROGRESS.name: {
                init_sequence: self.stats_metadata_collection,
                # TODO: This callback immediately transitions to the stats metadata collection completed event
                #  and should be updated appropriately when stats_metadata_collection is implemented
                states[STATS_METADATA_COLLECTION_STARTED].state_sequence: self.stats_metadata_collection_completed,
                states[STATS_METADATA_COLLECTION_COMPLETED].state_sequence: self.compaction_session,
                states[COMPACTION_SESSION_STARTED].state_sequence: self.compaction_session_completed,
                states[COMPACTION_SESSION_PARTITION_COMPLETED].state_sequence: self.compaction_session_completed,
                states[COMPACTION_SESSION_COMPLETED].state_sequence: self.complete_job
            },
        }

    def start_initializing(self):
        """Publish a job state event that indicates that a request to start a job run has been successfully received,
                but the job run has not yet finished prerequisite initialization steps.
        """
        self.event_dispatcher.dispatch_event(ScriptStartedEvent.start_initializing)

    def in_progress(self):
        """Publish a job state event that indicates that the job run is executing the Ray app and is in progress.
        """
        self.event_dispatcher.dispatch_event(ScriptInProgressEvent.in_progress)

    def stats_metadata_collection(self):
        """Publish a job state event that indicates that stats metadata collection has started.
        """
        event = self._event_map[STATS_METADATA_COLLECTION_STARTED]
        self.event_dispatcher.dispatch_event(event,
                                             event_data={
                                                 "eventName": event.name,
                                                 "stateDetailDescription": "Running stats metadata session",
                                             })

    def stats_metadata_collection_completed(self):
        """Publish a job state event that indicates that stats metadata collection is complete.
        """
        event = self._event_map[STATS_METADATA_COLLECTION_COMPLETED]
        self.event_dispatcher.dispatch_event(event,
                                             event_data={
                                                 "eventName": event.name,
                                                 "stateDetailDescription": "Finished collecting stats metadata",
                                             })

    def compaction_session(self):
        """Publish a job state event that indicates that the compaction run has started.
        """
        event = self._event_map[COMPACTION_SESSION_STARTED]
        self.event_dispatcher.dispatch_event(event,
                                             event_data={
                                                 "eventName": event.name,
                                                 "stateDetailDescription": "Running compaction session",
                                             })
        if self.session_manager:
            self.session_manager.launch_compaction()

    def compaction_partition_completed(self, partition_id: str):
        """Publish a job state event that indicates that a single partition has finished compaction.
        A compaction session can have 1...N partitions to compact.
        """
        event = self._event_map[COMPACTION_SESSION_PARTITION_COMPLETED]
        self.event_dispatcher.dispatch_event(event,
                                             event_data={
                                                 "eventName": event.name,
                                                 "stateDetailDescription": f"Finished compaction on "
                                                                           f"partition stream UUID: {partition_id}",
                                                 "stateDetailMetadata": {"partition_id": partition_id}
                                             })

    def compaction_session_completed(self):
        """Publish a job state event that indicates that the compaction run is complete.
        """
        if self.event_store is None or self.partition_ids_to_compact is None:
            # TODO: Separate this workflow out into multiple workflows, for different applications
            raise ValueError("Event Store and partition IDs must be provided.")

        partition_ids_completed = set(self.event_store.get_compacted_partition_ids())
        if partition_ids_completed == self.partition_ids_to_compact:
            logger.info(f"Compaction run complete.")
            event = self._event_map[COMPACTION_SESSION_COMPLETED]
            self.event_dispatcher.dispatch_event(event,
                                                 event_data={
                                                     "eventName": event.name,
                                                     "stateDetailDescription": "Finished compaction run",
                                                 })
        else:
            logger.info(f"Compaction is in progress: {len(partition_ids_completed)} "
                        f"out of {len(self.partition_ids_to_compact)} partitions completed...")

    def complete_job(self):
        """Publish a job state event that indicates that the job run has completed.
        """
        self.event_dispatcher.dispatch_event(ScriptCompletedEvent.completed)

    @property
    def session_manager(self):
        return self.event_dispatcher.session_manager


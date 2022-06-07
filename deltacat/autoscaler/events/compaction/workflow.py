import json
import logging
from typing import List, Union, Dict, Set, Any

from deltacat import logs
from deltacat.autoscaler.events.compaction.input import CompactionInput
from deltacat.autoscaler.events.compaction.process import CompactionProcess
from deltacat.autoscaler.events.compaction.utils import calc_compaction_cluster_memory_bytes, get_compaction_size_inputs
from deltacat.autoscaler.events.event_store import EventStoreClient
from deltacat.autoscaler.events.workflow import EventWorkflow, StateTransitionMap
from deltacat.autoscaler.events.compaction.dispatcher import CompactionEventDispatcher
from deltacat.autoscaler.events.states import ScriptStartedEvent, ScriptInProgressEvent, \
    ScriptCompletedEvent, States, ScriptInProgressCustomEvent
from deltacat.storage import PartitionLocator

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


STATS_METADATA_COLLECTION_STARTED = "STATS_METADATA_COLLECTION_STARTED"
STATS_METADATA_COLLECTION_COMPLETED = "STATS_METADATA_COLLECTION_COMPLETED"
COMPACTION_SESSION_STARTED = "COMPACTION_SESSION_STARTED"
COMPACTION_SESSION_PARTITION_COMPLETED = "COMPACTION_SESSION_PARTITION_COMPLETED"
COMPACTION_SESSION_COMPLETED = "COMPACTION_SESSION_COMPLETED"


# TODO: Might be worth renaming this later to avoid confusion with Ray Workflows
class CompactionWorkflow(EventWorkflow):

    def __init__(self,
                 config: Dict[str, Any],
                 event_dispatcher: CompactionEventDispatcher,
                 event_store: EventStoreClient = None,
                 compaction_inputs: List[CompactionInput] = None):
        self.config = config
        self.event_dispatcher = event_dispatcher
        assert self.event_dispatcher is not None, f"Event dispatcher must be provided to build and transition " \
                                                  f"to different job states."
        self.event_store = event_store
        self._compaction_inputs = compaction_inputs

        # Initialization
        self._build_states()
        self.state_transitions = self._build_state_transitions()

        if compaction_inputs is None:
            compaction_inputs = []
        compaction_source_partition_locators = [task.source_partition_locator for task in compaction_inputs]
        compaction_source_partition_ids = [loc.partition_id for loc in compaction_source_partition_locators]
        self._partition_ids_to_compact = set(compaction_source_partition_ids)
        self._metastats = {}

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
        in_progress_sequence = {name: event.state_sequence for name, event in self._event_map.items()}
        return {
            States.STARTED.name: self.mark_in_progress,
            States.IN_PROGRESS.name: {
                init_sequence: self.begin_stats_metadata_collection,
                in_progress_sequence[STATS_METADATA_COLLECTION_COMPLETED]: self.begin_compaction,
                in_progress_sequence[COMPACTION_SESSION_STARTED]: self.wait_or_mark_compaction_complete,
                in_progress_sequence[COMPACTION_SESSION_PARTITION_COMPLETED]: self.wait_or_mark_compaction_complete,
                in_progress_sequence[COMPACTION_SESSION_COMPLETED]: self.complete_job
            },
        }

    def start_initializing(self):
        """Publish a job state event that indicates that a request to start a job run has been successfully received,
                but the job run has not yet finished prerequisite initialization steps.
        """
        self.event_dispatcher.dispatch_event(ScriptStartedEvent.start_initializing)

    def mark_in_progress(self):
        """Publish a job state event that indicates that the job run is executing the Ray app and is in progress.
        """
        self.event_dispatcher.dispatch_event(ScriptInProgressEvent.in_progress)

    def begin_stats_metadata_collection(self):
        """Publish a job state event that indicates that stats metadata collection has started.
        """
        event = self._event_map[STATS_METADATA_COLLECTION_STARTED]
        self.event_dispatcher.dispatch_event(event,
                                             event_data={
                                                 "eventName": event.name,
                                                 "stateDetailDescription": "Running stats metadata session",
                                             })
        if self.session_manager:
            self._metastats = self.session_manager.launch_stats_metadata_collection(
                [compact.source_partition_locator for compact in self._compaction_inputs]
            )
            self.stats_metadata_collection_completed()

    def stats_metadata_collection_completed(self):
        """Publish a job state event that indicates that stats metadata collection is complete.
        """
        event = self._event_map[STATS_METADATA_COLLECTION_COMPLETED]
        self.event_dispatcher.dispatch_event(event,
                                             event_data={
                                                 "eventName": event.name,
                                                 "stateDetailDescription": "Finished collecting stats metadata",
                                             })

    def begin_compaction(self):
        """Publish a job state event that indicates that the compaction run has started.
        """
        event = self._event_map[COMPACTION_SESSION_STARTED]
        self.event_dispatcher.dispatch_event(event,
                                             event_data={
                                                 "eventName": event.name,
                                                 "stateDetailDescription": "Running compaction session",
                                             })
        if self.session_manager:
            processes = self.build_compaction_processes()
            self.session_manager.launch_compaction(processes)

    def build_compaction_processes(self) -> List[CompactionProcess]:
        processes = []
        partition_stats_metadata = self._metastats
        for compaction_input in self._compaction_inputs:
            stats_metadata = partition_stats_metadata.get(compaction_input.source_partition_locator.partition_id, {})
            stats_metadata = {stream_pos: delta_stats for stream_pos, delta_stats in stats_metadata.items()
                              if stream_pos <= compaction_input.last_stream_position_to_compact}
            total_pyarrow_table_bytes = sum([stats_result.stats.pyarrow_table_bytes
                                             for stream_pos, stats_result in stats_metadata.items()
                                             if stats_result.stats is not None])
            cluster_memory_bytes = calc_compaction_cluster_memory_bytes(compaction_input, total_pyarrow_table_bytes)
            new_hash_bucket_count, yaml_file = get_compaction_size_inputs(self.config,
                                                                          compaction_input.partition_key_values,
                                                                          cluster_memory_bytes,
                                                                          stats_metadata=stats_metadata)
            compaction_process = CompactionProcess(compaction_input.source_partition_locator,
                                                   yaml_file.name,
                                                   new_hash_bucket_count,
                                                   compaction_input.last_stream_position_to_compact,
                                                   compaction_input.partition_key_values,
                                                   cluster_memory_bytes=cluster_memory_bytes,
                                                   input_delta_total_bytes=total_pyarrow_table_bytes)

            # TODO: Increase file descriptor limit on host (up to ~60k)
            # TODO: Emit metrics for compaction jobs with very high number of partitions
            processes.append(compaction_process)
        return processes

    def compaction_partition_completed(self, partition_locator: PartitionLocator):
        """Publish a job state event that indicates that a single partition has finished compaction.
        A compaction session can have 1...N partitions to compact.
        """
        event = self._event_map[COMPACTION_SESSION_PARTITION_COMPLETED]
        partition_id = partition_locator.partition_id
        self.event_dispatcher.dispatch_event(event,
                                             event_data={
                                                 "eventName": event.name,
                                                 "stateDetailDescription": f"Finished compaction on "
                                                                           f"partition stream UUID: {partition_id}",
                                                 "stateDetailMetadata": {
                                                     "partition_id": partition_id,
                                                     "partition_values": str(partition_locator.partition_values)
                                                 }
                                             })

    def wait_or_mark_compaction_complete(self):
        """Publish a job state event that indicates that the compaction run is complete.
        """
        if self.event_store is None or self._partition_ids_to_compact is None:
            # TODO: Separate this workflow out into multiple workflows, for different applications
            raise ValueError("Event Store and partition IDs must be provided.")

        partition_ids_completed = set(self.event_store.get_compacted_partition_ids())
        if partition_ids_completed == self._partition_ids_to_compact:
            logger.info(f"Compaction run complete.")
            event = self._event_map[COMPACTION_SESSION_COMPLETED]
            self.event_dispatcher.dispatch_event(event,
                                                 event_data={
                                                     "eventName": event.name,
                                                     "stateDetailDescription": "Finished compaction run",
                                                 })
        else:
            logger.info(f"Compaction is in progress: {len(partition_ids_completed)} "
                        f"out of {len(self._partition_ids_to_compact)} partitions completed...")

    def complete_job(self):
        """Publish a job state event that indicates that the job run has completed.
        """
        self.event_dispatcher.dispatch_event(ScriptCompletedEvent.completed)

    @property
    def session_manager(self):
        return self.event_dispatcher.session_manager

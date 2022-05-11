import logging

from deltacat import logs
from deltacat.autoscaler.events.workflow import EventWorkflow, StateTransitionMap
from deltacat.autoscaler.events.compaction.dispatcher import CompactionEventDispatcher
from deltacat.autoscaler.events.states import ScriptStartedEvent, ScriptInProgressEvent, \
    stats_metadata_collection_started_event, stats_metadata_collection_completed_event, \
    compaction_session_started_event, compaction_session_completed_event, ScriptCompletedEvent, States

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class CompactionWorkflow(EventWorkflow):
    def __init__(self,
                 event_dispatcher: CompactionEventDispatcher):
        self.event_dispatcher = event_dispatcher
        assert self.event_dispatcher is not None, f"Event dispatcher must be provided to transition " \
                                                  f"to different workflow states."
        self.state_transitions = self._build_state_transitions()
        self.start_initializing()

    @property
    def state_transition_map(self) -> StateTransitionMap:
        return self.state_transitions

    def _build_state_transitions(self) -> StateTransitionMap:
        """Builds a mapping of event states to state transitioning callbacks, or
        a dictionary of state transitioning callbacks.

        If an event has state sequences, a dictionary of callbacks is provided
        with sequences as keys and callback functions as values.

        Returns: a map of event states to callbacks or a dictionary of callbacks
        """
        start_sequence = 0
        return {
            States.STARTED.name: self.in_progress,
            States.IN_PROGRESS.name: {
                start_sequence: self.stats_metadata_collection,
                # TODO: This callback immediately transitions to the stats metadata collection completed event
                #  and should be updated appropriately when stats_metadata_collection is implemented
                stats_metadata_collection_started_event.state_sequence: self.stats_metadata_collection_completed,
                stats_metadata_collection_completed_event.state_sequence: self.compaction_session,
                # Print logs until child compaction session completes
                compaction_session_started_event.state_sequence:
                    lambda *args: logger.info("Waiting for compaction session to succeed..."),
                compaction_session_completed_event.state_sequence: self.complete_job
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
        self.event_dispatcher.dispatch_event(stats_metadata_collection_started_event,
                                             event_data={
                                                 "customEventName": stats_metadata_collection_started_event.name,
                                                 "customDescription": "Running stats metadata session",
                                             })

    def stats_metadata_collection_completed(self):
        """Publish a job state event that indicates that stats metadata collection is complete.
        """
        self.event_dispatcher.dispatch_event(stats_metadata_collection_completed_event,
                                             event_data={
                                                 "customEventName": stats_metadata_collection_completed_event.name,
                                                 "customDescription": "Finished collecting stats metadata",
                                             })

    def compaction_session(self):
        """Publish a job state event that indicates that the compaction run has started.
        """
        if self.session_manager:
            self.session_manager.launch_compaction()

        self.event_dispatcher.dispatch_event(compaction_session_started_event,
                                             event_data={
                                                 "customEventName": compaction_session_started_event.name,
                                                 "customDescription": "Running compaction session",
                                             })

    def compaction_session_completed(self):
        """Publish a job state event that indicates that the compaction run is complete.
        """
        self.event_dispatcher.dispatch_event(compaction_session_completed_event,
                                             event_data={
                                                 "customEventName": compaction_session_completed_event.name,
                                                 "customDescription": "Finished compaction run",
                                             })

    def complete_job(self):
        """Publish a job state event that indicates that the job run has completed.
        """
        self.event_dispatcher.dispatch_event(ScriptCompletedEvent.completed)

    @property
    def session_manager(self):
        return self.event_dispatcher.session_manager


import uuid
import logging
from typing import Dict, Any, Optional, Union, Callable

from deltacat.autoscaler.events.session_launcher import SessionLauncher
from ray.autoscaler._private.aws.events import AwsEventManagerBase
from ray.autoscaler._private.event_system import RayEvent, ScriptStartedEvent, ScriptInProgressEvent, \
    ScriptInProgressCustomEvent, ScriptCompletedEvent, event_enum_values, States

from deltacat import logs
from deltacat.storage import interface as unimplemented_deltacat_storage

stats_metadata_collection_started_event = ScriptInProgressCustomEvent("STATS_METADATA_COLLECTION_STARTED", 1)
stats_metadata_collection_completed_event = ScriptInProgressCustomEvent("STATS_METADATA_COLLECTION_COMPLETED", 2)
compaction_session_started_event = ScriptInProgressCustomEvent("COMPACTION_SESSION_STARTED", 3)
compaction_session_completed_event = ScriptInProgressCustomEvent("COMPACTION_SESSION_COMPLETED", 4)
custom_events = [stats_metadata_collection_started_event,
                 stats_metadata_collection_completed_event,
                 compaction_session_started_event,
                 compaction_session_completed_event]


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class CompactionEventDispatcher:
    def __init__(self,
                 events_manager: AwsEventManagerBase,
                 deltacat_storage: unimplemented_deltacat_storage,
                 session_launcher: SessionLauncher = None,
                 session_id: Optional[str] = None,
                 parent_session_id: Optional[str] = None):
        """Constructor for the event dispatcher.

        Intended for usage by Ray parent and child clusters running BDT compaction workflows.
        This helper class is a wrapper around the events_manager for dispatching compaction workflow events
        annotated with event metadata for tracking purposes, such as parent and child Ray session IDs.

        Args:
            session_id: Ray application session ID
            events_manager: Events manager for publishing events through a cloud provider
            session_launcher: Manager for launching child Ray sessions
            deltacat_storage: Storage interface for deltacat
        """
        self.events_manager = events_manager
        self.session_launcher = session_launcher
        self.deltacat_storage = deltacat_storage

        if not session_id:
            session_id = str(uuid.uuid4())

        self.session_id = session_id
        self.parent_session_id = parent_session_id

        # Setup event callbacks in the constructor
        self.add_event_handlers()

    def dispatch_event(self,
                       event: RayEvent,
                       event_data: Optional[Dict[str, Any]] = None):
        """Generic helper method to dispatch Ray job events

        Args:
            event: Ray job event to dispatch
            parent_session_id: Session ID of the parent Ray job instance
            session_id: Session ID of the current Ray job instance
            event_data: Additional metadata for the given event. Optional.

        Returns:

        """
        if event_data is None:
            event_data = {}

        event_data["event"] = event
        event_data["rayParentSessionId"] = self.parent_session_id
        event_data["raySessionId"] = self.session_id

        logger.info(f"Dispatching event {event.name} with parent Ray session ID = {self.parent_session_id} "
                    f"and current Ray session ID = {self.session_id}")

        self._publish_event({
            **self.events_manager.config["parameters"],
            **event_data
        })

    def start_initializing(self):
        """Publish a job state event
        """
        self.dispatch_event(ScriptStartedEvent.start_initializing)

    def in_progress(self):
        """Publish a job state event
        """
        self.dispatch_event(ScriptInProgressEvent.in_progress)

    def stats_metadata_collection(self):
        """Publish a job state event
        """
        self.dispatch_event(stats_metadata_collection_started_event,
                            event_data={
                                "customEventName": stats_metadata_collection_started_event.name,
                                "customDescription": "Running stats metadata session",
                            })

    def stats_metadata_collection_completed(self):
        """Publish a job state event
        """
        self.dispatch_event(stats_metadata_collection_completed_event,
                            event_data={
                                "customEventName": stats_metadata_collection_completed_event.name,
                                "customDescription": "Finished collecting stats metadata",
                            })

    def compaction_session(self):
        """Publish a job state event
        """
        self._compact()
        self.dispatch_event(compaction_session_started_event,
                            event_data={
                                "customEventName": compaction_session_started_event.name,
                                "customDescription": "Running compaction session",
                            })

    def _compact(self):
        if self.session_launcher:
            self.session_launcher.compact(self.session_id)

    def compaction_session_completed(self):
        """Publish a job state event
        """
        self.dispatch_event(compaction_session_completed_event,
                            event_data={
                                "customEventName": compaction_session_completed_event.name,
                                "customDescription": "Finished compaction run",
                            })

    def complete_job(self):
        """Publish a job state event
        """
        self.dispatch_event(ScriptCompletedEvent.complete_success)

    def build_state_transitions(self) -> Dict[str, Union[Callable[[], None], Dict]]:
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

    def add_event_handlers(self):
        """Add callback handlers to job events
        """
        manager = self.events_manager
        if manager:
            for event in event_enum_values:
                logger.info(f"[{manager.__class__.__name__}]: Adding callback for event {event.name}")
                manager.add_callback(event)

            for event in custom_events:
                manager.add_callback(event)

    def _publish_event(self, event_data: Dict[str, Any]):
        manager = self.events_manager
        if manager and event_data and event_data.get("event"):
            event: RayEvent = event_data["event"]
            logger.info(f"[{manager.__class__.__name__}]: Publishing event {event.name}")
            manager.publish(event, event_data)

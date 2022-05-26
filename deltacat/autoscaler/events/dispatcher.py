import logging
from typing import Dict, Any, Optional, Callable, List

from deltacat.autoscaler.events.session_manager import SessionManager
from deltacat.autoscaler.events.states import event_enum_values
from ray.autoscaler._private.event_system import RayEvent, EventPublisher

from deltacat import logs
from deltacat.storage import interface as unimplemented_deltacat_storage

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class EventDispatcher:
    def __init__(self,
                 events_publisher: EventPublisher,
                 deltacat_storage: unimplemented_deltacat_storage,
                 session_manager: SessionManager = None):
        """Constructor for the event dispatcher.

        Intended for usage by Ray parent and child clusters running managed jobs.

        Args:
            events_publisher: Events manager for publishing events through a cloud provider
            session_manager: Manager for tracking and launching Ray sessions
            deltacat_storage: Storage interface for deltacat
        """
        self.events_publisher = events_publisher
        self.deltacat_storage = deltacat_storage
        self.session_manager = session_manager

        # Setup event callbacks in the constructor
        self._add_base_event_handlers()

    def dispatch_event(self,
                       event: RayEvent,
                       event_data: Optional[Dict[str, Any]] = None):
        """Generic helper method to dispatch Ray job events

        Args:
            event: Ray job event to dispatch
            event_data: Additional metadata for the given event. Optional.

        Returns:

        """
        if event_data is None:
            event_data = {}

        event_data["event"] = event
        if self.session_manager:
            event_data["rayParentSessionId"] = self.session_manager.parent_session_id
            event_data["raySessionId"] = self.session_manager.session_id

            logger.info(f"Dispatching event {event.name} "
                        f"with parent Ray session ID = {self.session_manager.parent_session_id} "
                        f"and current Ray session ID = {self.session_manager.session_id}")

        self._publish_event({
            **self.events_publisher.config["parameters"],
            **event_data
        })

    def _add_base_event_handlers(self):
        """Add callback handlers for base job events
        """
        publisher = self.events_publisher
        if publisher:
            for event in event_enum_values:
                logger.info(f"[{publisher.__class__.__name__}]: Adding callback for event {event.name}")
                publisher.add_callback(event)

    def add_event_handlers(self, custom_events: List[RayEvent]):
        """Add callback handlers for custom job events
        """
        publisher = self.events_publisher
        if publisher:
            for event in custom_events:
                logger.info(f"[{publisher.__class__.__name__}]: Adding callback for event {event.name}")
                publisher.add_callback(event)

    def _publish_event(self, event_data: Dict[str, Any]):
        publisher = self.events_publisher
        if publisher and event_data and event_data.get("event"):
            event: RayEvent = event_data["event"]
            logger.info(f"[{publisher.__class__.__name__}]: Publishing event {event.name}")
            publisher.publish(event, event_data)

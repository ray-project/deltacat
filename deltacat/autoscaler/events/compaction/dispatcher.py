import logging

from deltacat.autoscaler.events.compaction.session_manager import CompactionSessionManager
from deltacat.autoscaler.events.dispatcher import EventDispatcher
from ray.autoscaler._private.aws.events import EventPublisher

from deltacat import logs
from deltacat.storage import interface as unimplemented_deltacat_storage

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class CompactionEventDispatcher(EventDispatcher):
    def __init__(self,
                 events_publisher: EventPublisher,
                 deltacat_storage: unimplemented_deltacat_storage,
                 session_manager: CompactionSessionManager = None):
        """Constructor for the event dispatcher.

        Intended for usage by Ray parent and child clusters running compaction jobs.

        Args:
            events_publisher: Events manager for publishing events through a cloud provider
            deltacat_storage: Storage interface for deltacat
            session_manager: Manager for launching child Ray sessions
        """
        super().__init__(events_publisher, deltacat_storage, session_manager)
        self.session_manager = session_manager

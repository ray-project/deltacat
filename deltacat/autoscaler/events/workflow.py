import logging
from uuid import uuid4

import time
from typing import Dict, Callable, Union, Tuple, Optional

from ray.autoscaler._private.aws.events import AwsEventManagerBase

from deltacat import logs
from deltacat.autoscaler.events.dispatcher import EventDispatcher
from deltacat.autoscaler.events.event_store import EventStoreClient
from deltacat.autoscaler.events.exceptions import EventNotFoundException

logging.basicConfig(level=logging.INFO)
logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def poll_events(events_manager: AwsEventManagerBase,
                event_store: EventStoreClient,
                event_dispatcher: EventDispatcher,
                state_transition_map: Dict[str, Union[Callable[[], None], Dict]] = None):
    """Polls the event store and handles state transitions based on incoming events.

    Args:
        events_manager: Events manager for publishing events through a cloud provider
        event_store: High-level API client for the Event Store database
        event_dispatcher: Generates and publishes job state events
        state_transition_map: A mapping of event states to callbacks or a dictionary of callbacks.

    """
    # Generate new parent session ID
    parent_uuid = str(uuid4())
    events_manager.config["parameters"]["rayParentSessionId"] = parent_uuid
    logger.info(f"Start initializing...")

    if state_transition_map is None:
        state_transition_map = event_dispatcher.build_state_transitions()

    event_dispatcher.start_initializing()
    trace_id = events_manager.config["parameters"]["traceId"]
    dest_provider = events_manager.config["parameters"]["destinationTable"]["owner"]
    dest_table = events_manager.config["parameters"]["destinationTable"]["name"]
    expiry_timestamp = events_manager.config["parameters"]["expirationTimestamp"]
    while round(time.time() * 1000) < expiry_timestamp:
        logger.info(f"Polling latest job states for trace_id: {trace_id}, "
                    f"provider: {dest_provider} and table: {dest_table}...")
        try:
            state, state_sequence = get_latest_active_event(event_store, trace_id)
            to_next_state(state, state_sequence, state_transition_map)
        except EventNotFoundException as e:
            logger.error(e)

        time.sleep(20)


def get_latest_active_event(event_store: EventStoreClient,
                            trace_id: str) -> Tuple[Optional[str], int]:
    """

    Args:
        event_store: High-level API client for the Event Store database
        trace_id: Trace ID for a Ray Job

    Returns: tuple of state name (str) and the state sequence (int)

    """
    active_events = event_store.query_active_events(trace_id)
    latest_event = event_store.get_latest_event(active_events)
    if latest_event is None:
        raise EventNotFoundException(f"No events found for {trace_id}")

    latest_state, latest_state_sequence = latest_event["state"]["S"], int(latest_event["stateSequence"]["N"])
    return latest_state, latest_state_sequence


def to_next_state(event_state: str,
                  event_state_sequence: int,
                  state_transition_map: Dict[str, Union[Callable[[], None], Dict]]):
    """Reads the state_transition_map to execute the callback for the given event state and state sequence.

    Args:
        event_state: name of the job event state
        event_state_sequence: ID of the job event state sequence
        state_transition_map: A mapping of event states to callbacks or a dictionary of callbacks.

    """
    transition_cb = state_transition_map.get(event_state)

    if transition_cb is None:
        return

    if isinstance(transition_cb, dict):
        transition_sequence_cb: Callable[[], None] = transition_cb.get(event_state_sequence)
        if transition_sequence_cb and callable(transition_sequence_cb):
            logger.info(f"Calling function for {event_state} and sequence {event_state_sequence}")
            transition_sequence_cb()
    elif callable(transition_cb):
        transition_cb()


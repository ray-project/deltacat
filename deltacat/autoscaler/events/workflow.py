import logging

import time
from typing import Dict, Callable, Union, Tuple, Optional, List, Any

from botocore.exceptions import BotoCoreError
from ray.autoscaler._private.aws.events import AwsEventManagerBase
from ray.autoscaler._private.event_system import States

from deltacat import logs
from deltacat.autoscaler.events.dispatcher import CompactionEventDispatcher
from deltacat.autoscaler.events.event_store import EventStoreClient
from deltacat.autoscaler.events.exceptions import EventNotFoundException

logging.basicConfig(level=logging.INFO)
logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

QUERY_EVENTS_MAX_RETRY_COUNTER = 10
SLEEP_PERIOD_SECONDS = 20


# TODO: Make this the primary open-source Job Run Event Handler / Dispatcher?
#  Might be worth porting over some features of the Job Event Daemon (Java) to here

def poll_events(events_manager: AwsEventManagerBase,
                event_store: EventStoreClient,
                event_dispatcher: CompactionEventDispatcher,
                state_transition_map: Dict[str, Union[Callable[[], None], Dict]] = None):
    """Polls the event store and handles state transitions based on incoming events.

    This function will dispatch the STARTED event when first executed.
    The event listener will only listen to event states from STARTED and onwards.

    Event states before STARTED (i.e. NEW, DISPATCHED) are emitted from the Event Daemon (Java).

    Args:
        events_manager: Events manager for publishing events through a cloud provider
        event_store: High-level API client for the Event Store database
        event_dispatcher: Generates and publishes job state events
        state_transition_map: A mapping of event states to callbacks or a dictionary of callbacks.

    """
    logger.info(f"Start initializing...")

    if state_transition_map is None:
        state_transition_map = event_dispatcher.build_state_transitions()

    event_dispatcher.start_initializing()
    trace_id = events_manager.metadata["traceId"]
    dest_provider = events_manager.metadata["destinationTable"]["owner"]
    dest_table = events_manager.metadata["destinationTable"]["name"]
    expiry_timestamp = events_manager.metadata["expirationTimestamp"]

    retry_ctr = 0
    while round(time.time() * 1000) < expiry_timestamp and retry_ctr < QUERY_EVENTS_MAX_RETRY_COUNTER:
        logger.info(f"Polling latest job states for trace_id: {trace_id}, "
                    f"provider: {dest_provider} and table: {dest_table}...")

        try:
            events = event_store.query_events(trace_id)

            # Latest non-active / active event must be checked for the completed state.
            latest_state, latest_state_sequence = get_latest_event(events, trace_id)
            if latest_state == States.COMPLETED.name:
                logger.info("Completed Ray job! Exiting.")
                break

            # Latest active event must be checked for the next state transition
            latest_active_state, latest_active_state_sequence = get_latest_active_event(events, trace_id)

            # Uncomment for testing on non-active events to test specific steps of workflows
            # latest_active_state, latest_active_state_sequence = get_latest_event(events, trace_id)

            to_next_state(latest_active_state, latest_active_state_sequence, state_transition_map)

        except EventNotFoundException as e:
            logger.warning(e)
        except BotoCoreError as e:
            logger.error(e)
            retry_ctr += 1

        time.sleep(SLEEP_PERIOD_SECONDS)

    if retry_ctr == QUERY_EVENTS_MAX_RETRY_COUNTER:
        # TODO: Dispatch timeout event for IN_PROGRESS
        logger.error(f"Failed to fetch events for {trace_id} after "
                     f"{QUERY_EVENTS_MAX_RETRY_COUNTER} attempts")


def get_latest_event(events: List[Dict[str, Any]],
                     trace_id: str) -> Tuple[Optional[str], int]:
    """

    Args:
        events: Job events which may be active or non-active
        trace_id: Trace ID for a Ray Job

    Returns: tuple of state name (str) and the state sequence (int)

    """
    latest_event = EventStoreClient.get_latest_event(events)
    if latest_event is None:
        raise EventNotFoundException(f"No events found for Ray job: {trace_id}")

    latest_state, latest_state_sequence = latest_event["state"]["S"], int(latest_event["stateSequence"]["N"])
    return latest_state, latest_state_sequence


def get_latest_active_event(events: List[Dict[str, Any]],
                            trace_id: str) -> Tuple[Optional[str], int]:
    """

    Args:
        events: Job events which may be active or non-active
        trace_id: Trace ID for a Ray Job

    Returns: tuple of state name (str) and the state sequence (int)

    """
    active_events = [x for x in events if x.get("active")]
    latest_event = EventStoreClient.get_latest_event(active_events)
    if latest_event is None:
        raise EventNotFoundException(f"No events found for Ray job: {trace_id}")

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


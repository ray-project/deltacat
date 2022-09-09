import logging
from abc import abstractmethod, ABC
from typing import Dict, Union, Callable

from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))
StateTransitionCallback = Callable[[], None]
StateTransitionMap = Dict[str, Union[StateTransitionCallback, Dict]]


class EventWorkflow(ABC):
    @property
    @abstractmethod
    def state_transition_map(self) -> StateTransitionMap:
        raise NotImplementedError("Method not implemented")

    @abstractmethod
    def _build_state_transitions(self) -> StateTransitionMap:
        """Builds a mapping of event states to state transitioning callbacks, or
        a dictionary of state transitioning callbacks.

        If an event has state sequences, a dictionary of callbacks is provided
        with sequences as keys and callback functions as values.

        Returns: a map of event states to callbacks or a dictionary of callbacks
        """
        raise NotImplementedError("Method not implemented")

    def to_next_state(self,
                      event_state: str,
                      event_state_sequence: int):
        """Reads the state_transition_map to execute the callback for the given event state and state sequence.

        Args:
            event_state: name of the job event state
            event_state_sequence: ID of the job event state sequence

        """
        transition_cb: Union[StateTransitionCallback, Dict[int, StateTransitionCallback]] = \
            self.state_transition_map.get(event_state)

        if transition_cb is None:
            logger.debug(f"No callback found for state: {event_state}, "
                         f"sequence ID: {event_state_sequence}")
            return

        if isinstance(transition_cb, dict):
            transition_sequence_cb: Callable[[], None] = transition_cb.get(event_state_sequence)
            if transition_sequence_cb and callable(transition_sequence_cb):
                logger.info(f"Calling function for {event_state} and sequence {event_state_sequence}")
                transition_sequence_cb()
        elif callable(transition_cb):
            transition_cb()

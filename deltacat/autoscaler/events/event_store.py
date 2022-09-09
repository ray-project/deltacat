from typing import List, Dict, Any, Optional


class EventStoreClient:
    def query_events(self,
                     trace_id: str) -> List[Optional[Dict[str, Any]]]:
        """Query active events by Trace ID

        Args:
            trace_id: Trace ID for the job

        Returns: list of events that are active

        """
        raise NotImplementedError("Method not implemented")

    def query_active_events_by_destination_job_table(self,
                                                     destination_job_table: str) -> List[Optional[Dict[str, Any]]]:
        """Query active events from the job destination table
        
        Args:
            destination_job_table: Destination table for jobs

        Returns: list of active events for the particular job

        """
        raise NotImplementedError("Method not implemented")

    def query_active_events_by_event_name(self,
                                          event_name: str) -> List[Optional[Dict[str, Any]]]:
        """Query active events from the event name index

        Args:
            event_name: Name of the job event state

        Returns: list of active events for the particular event name

        """
        raise NotImplementedError("Method not implemented")

    def get_compacted_partition_ids(self, trace_id: str) -> List[str]:
        """Retrieve all compacted partition IDs.

        Returns: list of all compacted partition IDs

        """
        raise NotImplementedError("Method not implemented")

    def get_failed_partition_ids(self, trace_id: str) -> List[str]:
        """Retrieve all partition IDs that failed compaction.

        Returns: list of all failed partition IDs

        """
        raise NotImplementedError("Method not implemented")

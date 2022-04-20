from typing import List, Dict, Any, Optional
from botocore.client import BaseClient
from ray.autoscaler._private.event_system import ScriptStartedEvent, ScriptInProgressEvent, ScriptCompletedEvent, States


class EventStoreClient:
    def __init__(self,
                 dynamodb_client: BaseClient,
                 table_name: str):
        self.dynamodb_client = dynamodb_client
        self.table_name = table_name

    def query_active_events(self,
                            trace_id: str) -> List[Optional[Dict[str, Any]]]:
        """Query active events by Trace ID

        Args:
            trace_id: Trace ID for the job

        Returns: list of events that are active

        """
        result = self.dynamodb_client.query(
            TableName=self.table_name,
            ScanIndexForward=False,  # descending order traversal
            KeyConditions={
                "traceId": {
                    "AttributeValueList": [
                        {
                            "S": trace_id
                        },
                    ],
                    "ComparisonOperator": "EQ"
                },
            },
        )
        return self.get_active_events(result)

    def query_active_events_by_destination_job_table(self,
                                                     destination_job_table: str) -> List[Optional[Dict[str, Any]]]:
        """Query active events from the job destination table index
        
        Args:
            destination_job_table: Destination table for jobs

        Returns: list of active events for the particular job

        """
        result = self.dynamodb_client.query(
            TableName=self.table_name,
            IndexName="destinationTable.timestamp",
            ScanIndexForward=False,  # descending order traversal
            KeyConditions={
                "destinationTable": {
                    "AttributeValueList": [
                        {
                            "S": destination_job_table
                        },
                    ],
                    "ComparisonOperator": "EQ"
                },
            },
        )
        return self.get_active_events(result)

    def query_active_events_by_event_name(self,
                                          event_name: str) -> List[Optional[Dict[str, Any]]]:
        """Query active events from the event name index

        Args:
            event_name: Name of the job event state

        Returns: list of active events for the particular event name

        """
        result = self.dynamodb_client.query(
            TableName=self.table_name,
            IndexName="eventName.timestamp",
            KeyConditions={
                "eventName": {
                    "AttributeValueList": [
                        {
                            "S": event_name
                        },
                    ],
                    "ComparisonOperator": "EQ"
                },
            }
        )
        return self.get_active_events(result)

    @staticmethod
    def get_active_events(query_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Gets a filtered list of active job state events

        Args:
            query_result: list of job state events

        Returns: a filtered list of active job state events

        """
        items: List[Dict[str, Any]] = query_result["Items"]
        return [item for item in items if item.get("active")]

    @staticmethod
    def get_latest_event(items: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Get the latest event from the event store,

        Sorted by order of higher precedence - State, State Sequence ID, and finally Timestamp

        Args:
            items: list of job events as DynamoDB items

        Returns:
            event: job event sorted by State, State Sequence ID, and finally Timestamp
        """
        #
        latest_events = sorted(items,
                               key=lambda x: (States[x["state"]["S"]].value,
                                              x["stateSequence"]["N"],
                                              x["timestamp"]["N"]),
                               reverse=True)
        return latest_events[0] if latest_events else None

    def _get_active_running_events(self,
                                   query_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [item for item in self.get_active_events(query_result)
                if item["state"] in (ScriptStartedEvent.state, ScriptInProgressEvent.state, ScriptCompletedEvent.state)]

from typing import List, Dict, Any, Optional

import boto3
from botocore.client import BaseClient
from deltacat.autoscaler.events.compaction.workflow import COMPACTION_SESSION_PARTITIONS_COMPACTED, \
    COMPACTION_SESSION_PARTITIONS_FAILURE

from deltacat.autoscaler.events.event_store import EventStoreClient


class DynamoDBEventStoreClient(EventStoreClient):
    def __init__(self,
                 table_name: str,
                 dynamodb_client: BaseClient = None):
        if dynamodb_client is None:
            dynamodb_client = boto3.client("dynamodb", "us-east-1")

        self.dynamodb_client = dynamodb_client
        self.table_name = table_name

    def query_events(self,
                     trace_id: str) -> List[Optional[Dict[str, Any]]]:
        """Query events by Trace ID

        Args:
            trace_id: Trace ID for the job

        Returns: list of events that are active

        """
        return self.get_events(self._query_events(trace_id))

    def query_active_events(self,
                            trace_id: str) -> List[Optional[Dict[str, Any]]]:
        """Query active events by Trace ID

        Args:
            trace_id: Trace ID for the job

        Returns: list of events that are active

        """
        return self.get_active_events(self._query_events(trace_id))

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

    def get_compacted_partition_ids(self, trace_id: str) -> List[str]:
        items = self._get_completed_partition_events(trace_id)
        partition_id_list = [partition_id for event in items
                             if "stateDetailMetadata" in event
                             for partition_id in event["stateDetailMetadata"]["M"].keys()]
        return partition_id_list

    def get_failed_partition_ids(self, trace_id: str) -> List[str]:
        items = self._get_failed_partition_events(trace_id)
        partition_id_list = [partition_id for event in items
                             if "stateDetailMetadata" in event
                             for partition_id in event["stateDetailMetadata"]["M"].keys()]
        return partition_id_list

    @staticmethod
    def get_events(query_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Gets a filtered list of active job state events

        Args:
            query_result: list of job state events

        Returns: a filtered list of active job state events

        """
        return query_result["Items"]

    @staticmethod
    def get_active_events(query_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Gets a filtered list of active job state events

        Args:
            query_result: list of job state events

        Returns: a filtered list of active job state events

        """
        return [item for item in query_result["Items"] if item.get("active")]

    def _get_completed_partition_events(
            self,
            trace_id: str) -> List[Dict[str, Any]]:
        return [item for item in self.query_active_events(trace_id)
                if item["eventName"]["S"] == COMPACTION_SESSION_PARTITIONS_COMPACTED]

    def _get_failed_partition_events(
            self,
            trace_id: str) -> List[Dict[str, Any]]:
        return [item for item in self.query_active_events(trace_id)
                if item["eventName"]["S"] == COMPACTION_SESSION_PARTITIONS_FAILURE]

    def _query_events(self, trace_id: str):
        return self.dynamodb_client.query(
            TableName=self.table_name,
            IndexName="traceId.timestamp",
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


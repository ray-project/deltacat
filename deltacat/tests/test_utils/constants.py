import json
from deltacat.storage import Delta
from deltacat.tests.test_utils.attribute_dict import AttrDict

test_delta_file = open("deltacat/tests/test_utils/resources/test_delta.json")
test_delta_dict = json.load(test_delta_file)

TEST_DELTA = Delta(test_delta_dict)

TABLE_NAME = "test-table"
TABLE_VERSION = "1"
TABLE_PROVIDER = "test-provider"
TABLE_STREAM_UUID = "test-stream"
TABLE_PARTITION_ID = "test-partition-id"
TABLE_ARN = (
    f"arn:foo:::provider/{TABLE_PROVIDER}/table/{TABLE_NAME}/version/{TABLE_VERSION}"
)
TABLE_PARTITION_LOCATOR = AttrDict(
    {
        "namespace": TABLE_PROVIDER,
        "partition_values": [],
        "table_name": TABLE_NAME,
        "table_version": TABLE_VERSION,
    }
)

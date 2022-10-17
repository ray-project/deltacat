import ray
import sungate as sg
import logging

from deltacat.types.media import StorageType, ContentType, ContentEncoding, \
    DELIMITED_TEXT_CONTENT_TYPES
from sungate.storage.andes import EquivalentTableType

from deltacat.utils.performance import timed_invocation
from deltacat.storage import PartitionLocator, StreamLocator, TableVersionLocator, TableLocator, NamespaceLocator
from deltacat.aws.clients import resource_cache, client_cache
from deltacat.compute.compactor import RoundCompletionInfo, compaction_session

from sungate.storage.andes.schema.utils import to_pyarrow_schema

from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

from sungate.examples.scaling_test.constants import DEFAULT_COMPACTION_TEST_BUCKET_NAME, DEFAULT_COMPACTION_TEST_PROVIDER, SCALING_TEST_TABLE_NAME,SCALING_TEST_TABLE_VERSION,\
SCALING_TEST_STREAM_ID, SCALING_TEST_PARTITION_KEYS, SCALING_TEST_STORAGE, SCALING_TEST_PARTITION_VALUES, SCALING_TEST_DESTINATION_PROVIDER, SCALING_TEST_DESTINATION_TABLE_NAME

ray.init(address="auto")

def run_scaling_test(deltacat_storage, partition_values):
    client = client_cache("sts", None)
    account_id = client.get_caller_identity()["Account"]
    compaction_output_bucket_name = DEFAULT_COMPACTION_TEST_BUCKET_NAME + account_id
    s3 = resource_cache("s3", None)
    bucket = s3.Bucket(f"{compaction_output_bucket_name}")
    if not bucket.creation_date:
        logger.debug(f"Creating S3 bucket: {compaction_output_bucket_name}")
        s3.create_bucket(Bucket=compaction_output_bucket_name)
        logger.debug(f"S3 bucket created: {compaction_output_bucket_name}")

    # arrow schema?
    # //get_table_version instead. schema.

    ion_sdl_schema = deltacat_storage.get_table_version_schema(
        DEFAULT_COMPACTION_TEST_PROVIDER,
        SCALING_TEST_TABLE_NAME,
    )

    arrow_schema = to_pyarrow_schema(ion_sdl_schema)

    table_version = deltacat_storage.get_table_version(
        DEFAULT_COMPACTION_TEST_PROVIDER,
        SCALING_TEST_TABLE_NAME,
        SCALING_TEST_TABLE_VERSION
    )

    #
    compacted_table_version = deltacat_storage.create_table_version(
        SCALING_TEST_DESTINATION_PROVIDER,
        table_name=SCALING_TEST_DESTINATION_TABLE_NAME,
        schema=ion_sdl_schema,
        partition_keys=SCALING_TEST_PARTITION_KEYS,
        primary_key_column_names=table_version.primary_keys,
        supported_content_types=[ContentType.PARQUET],
        table_description="For scaling testing purposes only",
        always_create=False,
        deltacat_storage=deltacat_storage
    )
    compacted_partition_locators = []
    for partition_value in partition_values:
        compacted_partition_locators.append(
            PartitionLocator.of(
                compacted_table_version.locator,
                partition_value,
                None,  # partition ID
            )
        )

    stream_locator = StreamLocator.of(
                    TableVersionLocator.of(
                        TableLocator.of(
                            NamespaceLocator.of(
                                DEFAULT_COMPACTION_TEST_PROVIDER
                            ),
                            SCALING_TEST_TABLE_NAME,
                        ),
                        SCALING_TEST_TABLE_VERSION
                    ),
                    SCALING_TEST_STREAM_ID,
                    None
                )
    source_locators = []
    for pv in partition_values:
        actual_partition = deltacat_storage.get_partition(
           stream_locator,
            pv,
            equivalent_table_types=[EquivalentTableType.UNCOMPACTED],
        )
        source_locators.append(actual_partition.locator)

    res_obj_ref_dict = {}
    for i in range(len(source_locators)):
        print(source_locators[i].partition_values)
        res_obj_ref_dict[source_locators[i].partition_values[0]] = invoke_parallel_compact_partition.remote(
                    actual_partition,
                    source_locators[i],
                    compacted_partition_locators[i],
                    table_version,
                    compaction_output_bucket_name,
                    arrow_schema,
                    deltacat_storage)
    res_dict = {}
    print(res_obj_ref_dict)
    for k, v in res_obj_ref_dict.items():
        latency = ray.get(v)
        res_dict[k] = latency
    print(res_dict)


@ray.remote(num_cpus=1)
def invoke_parallel_compact_partition(actual_partition,
                                      source_locator,
                                      compacted_partition_locator,
                                      table_version,
                                      compaction_output_bucket_name,
                                      arrow_schema,
                                      deltacat_storage):
    res, latency = timed_invocation(
        func=compaction_session.compact_partition,
        source_partition_locator=source_locator,
        compacted_partition_locator=compacted_partition_locator,
        primary_keys=set(table_version.primary_keys),
        compaction_artifact_s3_bucket=compaction_output_bucket_name,
        last_stream_position_to_compact=actual_partition.stream_position,
        schema_on_read=arrow_schema,
        deltacat_storage=deltacat_storage
    )
    return res, latency


if __name__ == '__main__':
    run_scaling_test(SCALING_TEST_STORAGE, SCALING_TEST_PARTITION_VALUES)

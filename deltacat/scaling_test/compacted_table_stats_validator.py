import ray
import sungate as sg
import logging
import re

from deltacat.types.media import StorageType, ContentType, ContentEncoding, \
    DELIMITED_TEXT_CONTENT_TYPES
from sungate.storage.andes import EquivalentTableType

from deltacat.utils.performance import timed_invocation
from deltacat.storage import PartitionLocator, StreamLocator, TableVersionLocator, TableLocator, NamespaceLocator
from deltacat.aws.clients import resource_cache, client_cache
from deltacat.compute.compactor import RoundCompletionInfo, compaction_session

from sungate.storage.andes.schema.utils import to_pyarrow_schema
from deltacat.autoscaler.node_group import NodeGroupManager as ngm
from deltacat import logs

from sungate.storage.andes import PartitionKey, PartitionKeyType
from deltacat.compute.metastats.meta_stats import collect_from_partition, collect_metastats

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

ray.init(address="auto")

# Test script for validating compaction result, namely column count, row count, partitions count and in-memory Pyarrow bytes.
# Comparing between Ray and Spark compacted partitions. List ALL partitions in Ray compacted table version and get equivalent Spark partitions.
# Note: Things to double check for correct results:
# 1. Providers of table for your compacted table and Spark compacted table
# 2. Table name and Table version for your compacted table and Spark compacted table
# 3. Did we write to a new destination table and replace the SCALING_TEST_DESTINATION_TABLE_NAME with new destination table name
# 4. Did we release a new version after one compaction test and replace SCALING_TEST_DESTINATION_TABLE_VERSION with new table version
# 5. Is compaction test only reading from previous round completion files?

def valid_stats_for_compacted_table(deltacat_storage):

    # Note: For columns check, table version needs to be ACTIVE

    # ray_compacted_column_names = deltacat_storage.get_table_version_column_names(
    #     namespace=SCALING_TEST_DESTINATION_PROVIDER,
    #     table_name=SCALING_TEST_DESTINATION_TABLE_NAME,
    #     equivalent_table_types=[EquivalentTableType.COMPACTED_PARQUET]
    # )
    # spark_column_names = deltacat_storage.get_table_version_column_names(
    #     namespace=SCALING_TEST_TABLE_PROVIDER_COMPACTED,
    #     table_name=SPARK_COMPACTED_TABLE_NAME,
    #     equivalent_table_types=[EquivalentTableType.COMPACTED_PARQUET]
    # )
    #
    # original_table_column_names = deltacat_storage.get_table_version_column_names(
    #     namespace=DEFAULT_COMPACTION_TEST_PROVIDER,
    #     table_name=SCALING_TEST_TABLE_NAME,
    #     equivalent_table_types=[EquivalentTableType.UNCOMPACTED]
    # )
    # # number of columns should be same
    # # TODO(zyiqin): add check for primary key columns
    # print(f"Ray compacted table has {len(ray_compacted_column_names)} columns")
    # print(f"Spark compacted table has {len(spark_column_names)} columns")
    # print(len(original_table_column_names))
    # assert len(ray_compacted_column_names) == len(spark_column_names) == len(original_table_column_names)


    # TEST for all partitions
    partitions_list_res = deltacat_storage.list_partitions(
        namespace=SCALING_TEST_DESTINATION_PROVIDER,
        table_name=SCALING_TEST_DESTINATION_TABLE_NAME,
        equivalent_table_types=[EquivalentTableType.COMPACTED_PARQUET],
        table_version=SCALING_TEST_DESTINATION_TABLE_VERSION
    ).all_items()
    print(f"Got {len(partitions_list_res)} Ray partitions!")


    # Test for one partition
    # partitions_list_res = deltacat_storage.list_partitions(
    #     namespace=SCALING_TEST_DESTINATION_PROVIDER,
    #     table_name=SCALING_TEST_DESTINATION_TABLE_NAME,
    #     equivalent_table_types=[EquivalentTableType.COMPACTED_PARQUET],
    #     table_version="1"
    # ).read_page()[0]
    # pv = partitions_list_res.locator.partition_values


    spark_compacted_stream_locator = StreamLocator.of(
        TableVersionLocator.of(
            TableLocator.of(
                NamespaceLocator.of(
                    SCALING_TEST_TABLE_PROVIDER_COMPACTED,
                ),
                SPARK_COMPACTED_TABLE_NAME,
            ),
            SPARK_COMPACTED_TABLE_VERSION
        ),
        None,
        None
    )

    pvs = []
    ray_actual_partition_locators = []
    for partition in partitions_list_res:
        ray_actual_partition_locators.append(partition.locator)
        pvs.append(partition.locator.partition_values[1])

    print(f"Collecting stats on {len(ray_actual_partition_locators)} Ray partitions")

    spark_actual_partition_locators = []
    for pv in pvs:
        spark_pl = deltacat_storage.get_partition(
               spark_compacted_stream_locator,
                partition_values=[PARTITION_REGION, pv],
                equivalent_table_types=[EquivalentTableType.COMPACTED_PARQUET],
            )
        if spark_pl:
            spark_actual_partition_locators.append(spark_pl.locator)
        else:
            print(f"Warning: Spark partition locator not found for {pv}")

    print(f"Collecting stats on {len(spark_actual_partition_locators)} Spark partitions")

    STREAM_POSITION_INTERVAL_RANGE = {(0, 2 ** 53)}
    client = client_cache("sts", None)
    s3 = resource_cache("s3", None)
    account_id = client.get_caller_identity()["Account"]
    bucket_name = f"stats-{account_id}-scaling-test"
    bucket = s3.Bucket(f"{bucket_name}")
    if not bucket.creation_date:
        logger.debug(f"Creating S3 bucket: {bucket_name}")
        s3.create_bucket(Bucket=bucket_name)
        logger.debug(f"S3 bucket created: {bucket_name}")

    metastats_bucket_name = f"metastats-{account_id}-scaling-test"
    bucket_meta = s3.Bucket(f"{metastats_bucket_name}")
    if not bucket.creation_date:
        logger.debug(f"Creating S3 bucket: {bucket_meta}")
        s3.create_bucket(Bucket=metastats_bucket_name)
        logger.debug(f"S3 bucket created: {bucket_meta}")

    res_ray, latency_ray = timed_invocation(
        func=collect_metastats,
        source_partition_locators=ray_actual_partition_locators,
        delta_stream_position_range_set=STREAM_POSITION_INTERVAL_RANGE,
        file_count_per_cpu=1000,
        stat_results_s3_bucket=bucket_name,
        metastats_results_s3_bucket=metastats_bucket_name,
        deltacat_storage=deltacat_storage
    )

    res_spark, latency_spark = timed_invocation(
        func=collect_metastats,
        source_partition_locators=spark_actual_partition_locators,
        delta_stream_position_range_set=STREAM_POSITION_INTERVAL_RANGE,
        file_count_per_cpu=1000,
        stat_results_s3_bucket=bucket_name,
        metastats_results_s3_bucket=metastats_bucket_name,
        deltacat_storage=deltacat_storage
    )

    print(f"Got Ray stats collection result for {res_ray.keys()} partition values")
    print(f"Detailed Ray stats collection result for {res_ray}")
    print(f"Got Spark stats collection result for {res_spark.keys()} partition value")
    print(f"Detailed Spark stats collection result for {res_spark}")

    for k, v in res_ray.items():
        if v[0] == res_spark[k][0]:
            print(f"Same row count observed for partition: {k}")
        elif v[0] != res_spark[k][0]:
            print(f"Warning: Different row count for partition {k}: ray compacted: {v[0]}; spark compacted: {res_spark[k][0]}")
        print(f"in-memory pyarrow bytes for partition {k}: ray compacted {v[1]}; spark compacted {res_spark[k][1]}")


if __name__ == '__main__':
    valid_stats_for_compacted_table(SCALING_TEST_STORAGE)
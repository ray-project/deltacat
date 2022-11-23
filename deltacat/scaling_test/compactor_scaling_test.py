import ray
import sungate as sg
import logging,time
import re
import sys

from deltacat.types.media import StorageType, ContentType, ContentEncoding, \
    DELIMITED_TEXT_CONTENT_TYPES
from sungate.storage.andes import EquivalentTableType

from deltacat.utils.performance import timed_invocation
from deltacat.storage import PartitionLocator, StreamLocator, TableVersionLocator, TableLocator, NamespaceLocator
from deltacat.aws.clients import resource_cache, client_cache
from deltacat.compute.compactor import RoundCompletionInfo, compaction_session

from sungate.storage.andes.schema.utils import to_pyarrow_schema
from deltacat.autoscaler.node_group import NodeGroupManager as ngm, PlacementGroupManager as  pgm
from deltacat import logs

from sungate.storage.andes import PartitionKey, PartitionKeyType

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

ray.init(address="auto")
INSTANCE_CPU=16
DEFAULT_COMPACTION_TEST_BUCKET_NAME = f"p0-compaction-output"
DEFAULT_COMPACTION_TEST_PROVIDER = "BOOKER"
SCALING_TEST_TABLE_NAME = "D_UNIFIED_CUST_SHIPMENT_ITEMS_CAIRNS"
SCALING_TEST_TABLE_VERSION = "14"
SCALING_TEST_STREAM_ID = "e7887002-0b19-439a-a9a9-2ac66807af43"
SCALING_TEST_PARTITION_KEYS = [
    PartitionKey.of("region_id", PartitionKeyType.STRING),
    PartitionKey.of("ship_day", PartitionKeyType.STRING)
]

partition_stats = {'12022-10-01T00:00:00.000Z': [149817852, 115774180646], 
'12022-10-02T00:00:00.000Z': [196026518, 151477072317], 
'12022-10-03T00:00:00.000Z': [183571044, 141880607292], 
'12022-10-04T00:00:00.000Z': [171030627, 134153273794], 
'12022-10-05T00:00:00.000Z': [164389309, 129934794808], 
'12022-10-06T00:00:00.000Z': [153382712, 119111081938], 
'12022-10-07T00:00:00.000Z': [142619377, 108652222437], 
'12022-10-08T00:00:00.000Z': [125321273, 95360825872], 
'12022-10-09T00:00:00.000Z': [113262391, 86338514299], 
'12022-10-10T00:00:00.000Z': [113262104, 86639111199], 
'12022-10-11T00:00:00.000Z': [197898104, 152164556823], 
'12022-10-12T00:00:00.000Z': [203445970, 156469644681],
'12022-10-13T00:00:00.000Z': [112277266, 86246532400], 
'12022-10-14T00:00:00.000Z': [108019002, 82891110538], 
'12022-10-15T00:00:00.000Z': [107262173, 82502840679], 
'12022-10-16T00:00:00.000Z': [128978416, 99609141550], 
'12022-10-17T00:00:00.000Z': [141531185, 109607576723],
'12022-10-18T00:00:00.000Z': [138847288, 106138457015],
'12022-10-19T00:00:00.000Z': [113970949, 87157153964], 
'12022-10-20T00:00:00.000Z': [115399448, 88290376636], 
'12022-10-21T00:00:00.000Z': [107664984, 82291069679], 
'12022-10-22T00:00:00.000Z': [104804753, 79750502530],
'12022-10-23T00:00:00.000Z': [126483108, 96295905964], 
'12022-10-24T00:00:00.000Z': [124207363, 94905675449], 
'12022-10-25T00:00:00.000Z': [118922861, 92041335333], 
'12022-10-26T00:00:00.000Z': [113816153, 87238900886], 
'12022-10-27T00:00:00.000Z': [106545058, 81649724672], 
'12022-10-28T00:00:00.000Z': [100651316, 77006443170], 
'12022-10-29T00:00:00.000Z': [97991507, 74825326517], 
'12022-10-30T00:00:00.000Z': [111350044, 85082307318], 
'12022-10-31T00:00:00.000Z': [98475671, 75420534272]}

SCALING_TEST_PARTITION_VALUES=[[k[0],k[1:]] for k in partition_stats.keys()]

SCALING_TEST_STORAGE = sg.andes
SCALING_TEST_DESTINATION_PROVIDER = "bdt-ray-dev"
SCALING_TEST_DESTINATION_TABLE_NAME = "TEST_ROOTLIU_D_UNIFIED_CUST_SHIPMENT_ITEMS_CAIRNS"

def run_scaling_test(deltacat_storage, partition_values, num_partitions=1, use_pg = True):
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
    actual_partitions = []
    for pv in partition_values:
        actual_partition = deltacat_storage.get_partition(
           stream_locator,
            pv,
            equivalent_table_types=[EquivalentTableType.UNCOMPACTED],
        )
        actual_partitions.append(actual_partition)
        source_locators.append(actual_partition.locator)

    res_obj_ref_dict = {}
    if use_pg:
        pg_configs = pgm(num_partitions, INSTANCE_CPU).pgs
        print("Successfully created %d placement groups for %d partitions"%(len(pg_configs),num_partitions))
        print("Running compaction for %d partitions"%(min(len(pg_configs),num_partitions)))
        for i in range(min(len(pg_configs),num_partitions)):
            print(source_locators[i].partition_values)
            node_group_res = None
            pg_config = pg_configs[i]
            res_obj_ref_dict[source_locators[i].partition_values[1]] = invoke_parallel_compact_partition.remote(
                        actual_partitions[i],
                        source_locators[i],
                        compacted_partition_locators[i],
                        table_version,
                        compaction_output_bucket_name,
                        pg_config,
                        arrow_schema,
                        deltacat_storage,
                        node_group_res)
    else:
        for i in range(num_partitions):
            print(source_locators[i].partition_values)
            node_group_res = None
            pg_config = None
            res_obj_ref_dict[source_locators[i].partition_values[1]] = invoke_parallel_compact_partition.remote(
                        actual_partitions[i],
                        source_locators[i],
                        compacted_partition_locators[i],
                        table_version,
                        compaction_output_bucket_name,
                        pg_config,
                        arrow_schema,
                        deltacat_storage,
                        node_group_res)   
    res_dict = {}
    print(res_obj_ref_dict)
    for k, v in res_obj_ref_dict.items():
        latency = ray.get(v)
        res_dict[k] = latency
    print(res_dict)


@ray.remote(num_cpus=0.01)
def invoke_parallel_compact_partition(actual_partition,
                                      source_locator,
                                      compacted_partition_locator,
                                      table_version,
                                      compaction_output_bucket_name,
                                      pg_config,
                                      arrow_schema,
                                      deltacat_storage,
                                      node_group_res):
    res, latency = timed_invocation(
        func=compaction_session.compact_partition,
        source_partition_locator=source_locator,
        compacted_partition_locator=compacted_partition_locator,
        primary_keys=set(table_version.primary_keys),
        compaction_artifact_s3_bucket=compaction_output_bucket_name,
        last_stream_position_to_compact=actual_partition.stream_position,
        pg_config=pg_config,
        schema_on_read=arrow_schema,
        deltacat_storage=deltacat_storage,
        node_group_res=node_group_res
    )
    return res, latency


if __name__ == '__main__':
    if len(sys.argv)>=2:
        num_partitions = int(sys.argv[1])
    else:
        num_partitions = 1
    start = time.time()
    run_scaling_test(SCALING_TEST_STORAGE, SCALING_TEST_PARTITION_VALUES,num_partitions)
    end = time.time()
    print("Total Time {}".format(end-start))

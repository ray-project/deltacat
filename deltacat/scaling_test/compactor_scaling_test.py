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
import binpacking

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

p0_table_list=[("D_UNIFIED_CUSTOMER_ORDER_ITEMS_CAIRNS",22,'BOOKER'),("D_COI_MONETARY_AMOUNTS_CAIRNS",27,'BOOKER'),("contribution_ddl_PI_REFUNDS_REVENUE", 6,'dmo-cairns'),("D_CUST_SHIPMENT_ITEM_PKGS_CAIRNS",5,'BOOKER'),("D_UNIFIED_CUST_SHIPMENT_ITEMS_CAIRNS",14,'BOOKER'),("D_CUSTOMER_ORDER_ITEM_DETAILS_CAIRNS", 18,'BOOKER'),("D_PROMOTION_ORDER_TXNS_CAIRNS",13,'BOOKER'),("D_CUSTOMER_ORDERS_CAIRNS",13,'BOOKER')]

ray.init(address="auto")
INSTANCE_CPU=32
DEFAULT_COMPACTION_TEST_BUCKET_NAME = f"p0-compaction-output"
DEFAULT_COMPACTION_TEST_PROVIDER = ""
SCALING_TEST_TABLE_NAME = ""
SCALING_TEST_TABLE_VERSION = ""
SCALING_TEST_STREAM_ID = ""
SCALING_TEST_PARTITION_VALUES=[]
SCALING_TEST_PARTITION_KEYS = []
SCALING_TEST_STORAGE = sg.andes
SCALING_TEST_DESTINATION_PROVIDER = "bdt-ray-dev"
SCALING_TEST_DESTINATION_TABLE_NAME = "TEST_ROOTLIU_P0_"

STREAM_SHARD_POSITION=[
("e7887002-0b19-439a-a9a9-2ac66807af43","17f13750-c23d-4cd8-b277-e3b83a0e6a49","1670288433207"),
("e7887002-0b19-439a-a9a9-2ac66807af43","26c68154-8d1d-430b-8d42-c741e0585283","1670288431065"),
("e7887002-0b19-439a-a9a9-2ac66807af43","2f085e89-f29a-4927-a262-964a694f3213","1670288429193"),
("e7887002-0b19-439a-a9a9-2ac66807af43","30ad4d7e-8101-45e7-a872-f818e79f2e16","1670288429525"),
("e7887002-0b19-439a-a9a9-2ac66807af43","42f460ed-0f47-4961-a1ea-f55b0826997d","1670288430025"),
("e7887002-0b19-439a-a9a9-2ac66807af43","456f8d0e-fa7f-4175-a5e7-6c614edbf5e4","1670288430166"),
("e7887002-0b19-439a-a9a9-2ac66807af43","4e73f187-cbd4-4683-8dfd-2dd96f021783","1670288430357"),
("e7887002-0b19-439a-a9a9-2ac66807af43","50423f78-f452-4f96-b9de-0bc3ce2b2e7b","1670288429520"),
("e7887002-0b19-439a-a9a9-2ac66807af43","51d2cbce-2d5f-458d-a5d9-9af0645ced12","1670288430239"),
("e7887002-0b19-439a-a9a9-2ac66807af43","58254518-c218-4d5c-b7c6-d14f2dabb374","1670288429178"),
("e7887002-0b19-439a-a9a9-2ac66807af43","5a5693b1-044b-4640-a683-51618f3b48bd","1670288433248"),
("e7887002-0b19-439a-a9a9-2ac66807af43","60e4f6fb-a1be-491b-bb8b-e20f44f7ea80","1670288429853"),
("e7887002-0b19-439a-a9a9-2ac66807af43","6ea63661-9d4f-4e36-a2bb-a7a6746fe967","1670288430330"),
("e7887002-0b19-439a-a9a9-2ac66807af43","70344cde-c90f-4684-805d-f6891fd614be","1670288433187"),
("e7887002-0b19-439a-a9a9-2ac66807af43","79b402aa-3c51-403f-a752-d01228b082c7","1670288429142"),
("e7887002-0b19-439a-a9a9-2ac66807af43","836e1b13-c1f3-453b-8318-e28f389e0854","1670288429392"),
("e7887002-0b19-439a-a9a9-2ac66807af43","838afd37-b45f-4ff9-8aa4-a54693cda167","1670288430178"),
("e7887002-0b19-439a-a9a9-2ac66807af43","867c9b66-9b93-496c-98dd-ad7bbedb0829","1670288429358"),
("e7887002-0b19-439a-a9a9-2ac66807af43","8d9d6092-9376-4759-b8af-5faaba5f5dbc","1670288429272"),
("e7887002-0b19-439a-a9a9-2ac66807af43","a67dfc17-6912-44a2-ab2f-d52437a01df5","1670288433015"),
("e7887002-0b19-439a-a9a9-2ac66807af43","bd69a378-4815-4d97-a024-f0c56a54b49c","1670288430026"),
("e7887002-0b19-439a-a9a9-2ac66807af43","be0581b5-de00-4301-b355-4d46a5be9095","1670288430045"),
("e7887002-0b19-439a-a9a9-2ac66807af43","d1f5ff07-98d2-499d-86ba-4a491846de54","1670288433395"),
("e7887002-0b19-439a-a9a9-2ac66807af43","d6519c11-6619-490d-a9f1-185c8fe39819","1670288429815"),
("e7887002-0b19-439a-a9a9-2ac66807af43","d6828ade-7486-4475-921b-1dcb28a7c31b","1670288430018"),
("e7887002-0b19-439a-a9a9-2ac66807af43","d6f54b7c-e4ea-4919-986d-25882cbf7131","1670288429247"),
("e7887002-0b19-439a-a9a9-2ac66807af43","db2d1ad9-a8c7-4d1a-818a-f7d9467b6a7c","1670288430226"),
("e7887002-0b19-439a-a9a9-2ac66807af43","dcc3fdf0-442a-47b7-9a43-0a55fa075164","1670288433330"),
("e7887002-0b19-439a-a9a9-2ac66807af43","ed1cc621-707c-4fca-93b5-d1940b24667f","1670288430072"),
("e7887002-0b19-439a-a9a9-2ac66807af43","fa3f9e9e-0c4c-4cb6-9f24-2c6f32f6f713","1670288429712"),
("e7887002-0b19-439a-a9a9-2ac66807af43","fba493b0-722c-4d1e-8af0-262778a56255","1670288430331")]

STREAM_SHARD_POSITION_DICT = {x[1]:int(x[2]) for x in STREAM_SHARD_POSITION}

SCALING_TEST_MAX_POSITIONS = []
PACKING_RATIO=2

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

    ion_sdl_schema = deltacat_storage.get_table_version_schema(
        DEFAULT_COMPACTION_TEST_PROVIDER,
        SCALING_TEST_TABLE_NAME,
    )
    ion_sdl_schema = ion_sdl_schema.replace("int,max_value","int,min_value:-99999999999999999999999999999999999999,max_value")
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
    print(f"Destination table version:{compacted_table_version}")
    compacted_partition_locators = {}
    for partition_value in partition_values:
        compacted_partition_locators["_".join(partition_value)]=(
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
    actual_partitions = {}
    for pv in partition_values:
        actual_partition = deltacat_storage.get_partition(
           stream_locator,
            pv,
            equivalent_table_types=[EquivalentTableType.UNCOMPACTED],
        )
        pkv="_".join(actual_partition.locator.partition_values)
        actual_partitions[pkv]=actual_partition
        source_locators.append(actual_partition.locator)

    res_obj_ref_dict = {}
    res_obj_ref_dict_reverse = {}
    num_bins = int(num_partitions/PACKING_RATIO)+1

    partition_load={'1_2022-10-01T00:00:00.000Z': 239.45530625599986,
     '1_2022-10-02T00:00:00.000Z': 264.7331537299997,
     '1_2022-10-03T00:00:00.000Z': 334.8085297300004,
     '1_2022-10-04T00:00:00.000Z': 297.6533000109994,
     '1_2022-10-05T00:00:00.000Z': 304.8239868319997,
     '1_2022-10-06T00:00:00.000Z': 318.27936804099954,
     '1_2022-10-07T00:00:00.000Z': 325.2521296570003,
     '1_2022-10-08T00:00:00.000Z': 266.6543092359998,
     '1_2022-10-09T00:00:00.000Z': 254.70581235600002,
     '1_2022-10-10T00:00:00.000Z': 340.09875922099945,
     '1_2022-10-11T00:00:00.000Z': 380.78238548399986,
     '1_2022-10-12T00:00:00.000Z': 425.0094892590005,
     '1_2022-10-13T00:00:00.000Z': 324.6910083920002,
     '1_2022-10-14T00:00:00.000Z': 334.327181396,
     '1_2022-10-15T00:00:00.000Z': 283.66673159899983,
     '1_2022-10-16T00:00:00.000Z': 267.3530355940002,
     '1_2022-10-17T00:00:00.000Z': 317.8078006799997,
     '1_2022-10-18T00:00:00.000Z': 292.6624743550001,
     '1_2022-10-19T00:00:00.000Z': 309.2933899969994,
     '1_2022-10-20T00:00:00.000Z': 293.1994929580005,
     '1_2022-10-21T00:00:00.000Z': 302.13728437900045,
     '1_2022-10-22T00:00:00.000Z': 247.02615472300022,
     '1_2022-10-23T00:00:00.000Z': 264.8268146269993,
     '1_2022-10-24T00:00:00.000Z': 313.4345854479998,
     '1_2022-10-25T00:00:00.000Z': 319.8865999030004,
     '1_2022-10-26T00:00:00.000Z': 269.24475705199984,
     '1_2022-10-27T00:00:00.000Z': 295.24841257999924,
     '1_2022-10-28T00:00:00.000Z': 253.0235409549996,
     '1_2022-10-29T00:00:00.000Z': 303.7226543349998,
     '1_2022-10-30T00:00:00.000Z': 261.30250930700004,
     '1_2022-10-31T00:00:00.000Z': 324.1171102770004}
    partition_bins = binpacking.to_constant_bin_number(partition_load,num_bins)
    print(f"Partition Bins:{partition_bins}")
    import copy
    partition_bins_copy = copy.deepcopy(partition_bins)
    if use_pg:
        pg_time=time.time()
        pg_configs = pgm(num_bins, INSTANCE_CPU).pgs
        print("Successfully created %d placement groups for %d partitions %.2f sec"%(len(pg_configs),num_partitions, time.time()-pg_time))
        print("Running compaction for %d partitions"%(min(len(pg_configs),num_partitions)))
        compacted=0
        compact_round=0
        done_list=[]
        done_keys_list=[]
        while compacted<num_partitions:
            print("Global Compaction Round %d"%compact_round)
            if len(res_obj_ref_dict.keys()):
                done_list, _ = ray.wait(list(res_obj_ref_dict.values()),num_returns=1+len(done_list))
                done_keys_list = [res_obj_ref_dict_reverse[objref] for objref in done_list]
                print("Number of Compaction Done:%d"%len(done_list))
            for i in range(min(len(pg_configs),num_partitions)):
                #print(source_locators[i].partition_values)
                bins_todo = partition_bins[i]
                key_bins_todo = list(bins_todo.keys())
                if len(key_bins_todo)==0:
                    continue
                bin_todo = key_bins_todo[0]
                #check if pg is released
                if compact_round>0:
                    bin_todo_brothers = set(list(partition_bins_copy[i].keys()))
                    done_keys_list_set = set(done_keys_list)                    
                    if not done_keys_list_set.intersection(bin_todo_brothers):
                        continue
                print(f"Running Compaction for {bin_todo}")
                compacted+=1
                del partition_bins[i][bin_todo] # running compaction in this round
                actual_partition = actual_partitions[bin_todo]
                node_group_res = None
                pg_config = pg_configs[i]
                last_stream_position_to_compact=actual_partition.stream_position
                pkv="_".join(actual_partition.locator.partition_values)
                assert pkv == bin_todo
                if SCALING_TEST_MAX_POSITIONS and pkv in SCALING_TEST_MAX_POSITIONS.keys():
                    last_stream_position_to_compact=SCALING_TEST_MAX_POSITIONS[pkv]
                res_obj_ref_dict[pkv] = invoke_parallel_compact_partition.remote(
                            actual_partition,
                            actual_partition.locator,
                            compacted_partition_locators[pkv],
                            table_version,
                            compaction_output_bucket_name,
                            pg_config,
                            arrow_schema,
                            deltacat_storage,
                            node_group_res,
                            last_stream_position_to_compact)
                res_obj_ref_dict_reverse[res_obj_ref_dict[pkv]]=pkv
            compact_round+=1
    else:
        for i in range(num_partitions):
            print(source_locators[i].partition_values)
            node_group_res = None
            pg_config = None
            pkv = "_".join(source_locators[i].partition_values)
            res_obj_ref_dict[pkv] = invoke_parallel_compact_partition.remote(
                        actual_partitions[pkv],
                        source_locators[i],
                        compacted_partition_locators[pkv],
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
def get_partitionvalues(deltacat_storage,provider,table,version,year,month,region):
    tableinfo = deltacat_storage.get_table_version(provider,table, version)
    table_keys = tableinfo['partitionKeys']
    streamid = tableinfo['properties']['cairnsStream']
    partitions = deltacat_storage.list_partitions(provider,table, version).all_items()
    partitionvalues=[]
    max_positions={}
    for p in partitions:
        y = p['partitionLocator']['partitionValues'][1].split('-')[0]
        m = p['partitionLocator']['partitionValues'][1].split('-')[1]
        r = p['partitionLocator']['partitionValues'][0]
        if y == year and m == month:
            if region is not None and str(r)==region:
                partitionvalues.append(p['partitionLocator']['partitionValues'])
            elif region==None:
                partitionvalues.append(p['partitionLocator']['partitionValues'])
            partition_id = p['partitionLocator']['partitionId']
            if partition_id in STREAM_SHARD_POSITION_DICT.keys():
                max_position = STREAM_SHARD_POSITION_DICT[partition_id]
                max_positions["_".join(p['partitionLocator']['partitionValues'])]=max_position
    return [provider, table, version, streamid, table_keys,partitionvalues,max_positions]


@ray.remote(num_cpus=0.01)
def invoke_parallel_compact_partition(actual_partition,
                                      source_locator,
                                      compacted_partition_locator,
                                      table_version,
                                      compaction_output_bucket_name,
                                      pg_config,
                                      arrow_schema,
                                      deltacat_storage,
                                      node_group_res,
                                      last_stream_position_to_compact):
    res, latency = timed_invocation(
        func=compaction_session.compact_partition,
        source_partition_locator=source_locator,
        compacted_partition_locator=compacted_partition_locator,
        primary_keys=set(table_version.primary_keys),
        compaction_artifact_s3_bucket=compaction_output_bucket_name,
        last_stream_position_to_compact=last_stream_position_to_compact,
        pg_config=pg_config,
        schema_on_read=arrow_schema,
        deltacat_storage=deltacat_storage,
        node_group_res=node_group_res
    )      
    return res, latency


# def get_bench_input():
#     table = sg.andes.get_table_version("BOOKER","D_CUSTOMER_ORDER_ITEM_DETAILS_CAIRNS","18")

if __name__ == '__main__':
    if len(sys.argv)>=5:
        #print("All args:",sys.argv)
        launching_time = float(sys.argv[1])
        number_partitions = int(sys.argv[2])
        test_table_id = int(sys.argv[3])
        number_cpus = int(sys.argv[4])
    else:
        print("length of arg:%d"%(len(sys.argv)))
        raise Exception("ray submit .yaml .py num_partitions launching_time test_table_id num_cpus")
    print(f"Submitting time is:{launching_time}")


    #wait for resources
    while True:
        current_cpus = ray.cluster_resources()['CPU']
        if current_cpus >= number_cpus:
            break
        else:
            print("Waiting for resources to be ready [%d-%.0f]"%(current_cpus,number_cpus))
            time.sleep(30)

    print('Resources Ready, Start generating compaction input')
    #generate input
    start0 = time.time()
    table = str(p0_table_list[test_table_id][0])
    version = str(p0_table_list[test_table_id][1])
    provider = p0_table_list[test_table_id][2]
    year='2022'
    month='10'
    region='1' # NA
    #compaction_input = ray.get(get_partitionvalues.remote(SCALING_TEST_STORAGE,provider,table,version,year, month,region))
    DEFAULT_COMPACTION_TEST_PROVIDER = provider
    SCALING_TEST_TABLE_NAME = table
    SCALING_TEST_TABLE_VERSION = version

    # SCALING_TEST_STREAM_ID = str(compaction_input[3])
    # SCALING_TEST_PARTITION_KEYS = compaction_input[4]
    # SCALING_TEST_PARTITION_VALUES=compaction_input[5]
    # SCALING_TEST_DESTINATION_TABLE_NAME = SCALING_TEST_DESTINATION_TABLE_NAME+table
    # SCALING_TEST_MAX_POSITIONS=compaction_input[6]

    SCALING_TEST_STREAM_ID = 'e7887002-0b19-439a-a9a9-2ac66807af43'
    SCALING_TEST_PARTITION_KEYS = [{'keyName': 'region_id', 'keyType': 'int'}, {'keyName': 'ship_day', 'keyType': 'timestamp'}]
    SCALING_TEST_PARTITION_VALUES=[['1', '2022-10-01T00:00:00.000Z'], ['1', '2022-10-02T00:00:00.000Z'], ['1', '2022-10-03T00:00:00.000Z'], ['1', '2022-10-04T00:00:00.000Z'], ['1', '2022-10-05T00:00:00.000Z'], ['1', '2022-10-06T00:00:00.000Z'], ['1', '2022-10-07T00:00:00.000Z'], ['1', '2022-10-08T00:00:00.000Z'], ['1', '2022-10-09T00:00:00.000Z'], ['1', '2022-10-10T00:00:00.000Z'], ['1', '2022-10-11T00:00:00.000Z'], ['1', '2022-10-12T00:00:00.000Z'], ['1', '2022-10-13T00:00:00.000Z'], ['1', '2022-10-14T00:00:00.000Z'], ['1', '2022-10-15T00:00:00.000Z'], ['1', '2022-10-16T00:00:00.000Z'], ['1', '2022-10-17T00:00:00.000Z'], ['1', '2022-10-18T00:00:00.000Z'], ['1', '2022-10-19T00:00:00.000Z'], ['1', '2022-10-20T00:00:00.000Z'], ['1', '2022-10-21T00:00:00.000Z'], ['1', '2022-10-22T00:00:00.000Z'], ['1', '2022-10-23T00:00:00.000Z'], ['1', '2022-10-24T00:00:00.000Z'], ['1', '2022-10-25T00:00:00.000Z'], ['1', '2022-10-26T00:00:00.000Z'], ['1', '2022-10-27T00:00:00.000Z'], ['1', '2022-10-28T00:00:00.000Z'], ['1', '2022-10-29T00:00:00.000Z'], ['1', '2022-10-30T00:00:00.000Z'], ['1', '2022-10-31T00:00:00.000Z']]
    SCALING_TEST_DESTINATION_TABLE_NAME = SCALING_TEST_DESTINATION_TABLE_NAME+table
    SCALING_TEST_MAX_POSITIONS={'1_2022-10-01T00:00:00.000Z': 1670288429712, '1_2022-10-02T00:00:00.000Z': 1670288430072, '1_2022-10-03T00:00:00.000Z': 1670288430330, '1_2022-10-04T00:00:00.000Z': 1670288429525, '1_2022-10-05T00:00:00.000Z': 1670288429178, '1_2022-10-06T00:00:00.000Z': 1670288430025, '1_2022-10-07T00:00:00.000Z': 1670288429272, '1_2022-10-08T00:00:00.000Z': 1670288429520, '1_2022-10-09T00:00:00.000Z': 1670288429193, '1_2022-10-10T00:00:00.000Z': 1670288429815, '1_2022-10-11T00:00:00.000Z': 1670288429247, '1_2022-10-12T00:00:00.000Z': 1670288430331, '1_2022-10-13T00:00:00.000Z': 1670288429142, '1_2022-10-14T00:00:00.000Z': 1670288430026, '1_2022-10-15T00:00:00.000Z': 1670288431065, '1_2022-10-16T00:00:00.000Z': 1670288430239, '1_2022-10-17T00:00:00.000Z': 1670288429392, '1_2022-10-18T00:00:00.000Z': 1670288430357, '1_2022-10-19T00:00:00.000Z': 1670288430166, '1_2022-10-20T00:00:00.000Z': 1670288430018, '1_2022-10-21T00:00:00.000Z': 1670288430178, '1_2022-10-22T00:00:00.000Z': 1670288430226, '1_2022-10-23T00:00:00.000Z': 1670288429853, '1_2022-10-24T00:00:00.000Z': 1670288429358, '1_2022-10-25T00:00:00.000Z': 1670288430045, '1_2022-10-26T00:00:00.000Z': 1670288433248, '1_2022-10-27T00:00:00.000Z': 1670288433015, '1_2022-10-28T00:00:00.000Z': 1670288433187, '1_2022-10-29T00:00:00.000Z': 1670288433207, '1_2022-10-30T00:00:00.000Z': 1670288433330, '1_2022-10-31T00:00:00.000Z': 1670288433395}
    print("Provider:%s\nTable:%s\nVersion:%s\n"%(provider,table,version))
    print("StreamID:%s\n"%(SCALING_TEST_STREAM_ID))
    print(f"PartitionKeys:{SCALING_TEST_PARTITION_KEYS}\n")
    print("Destination Table:%s\n"%SCALING_TEST_DESTINATION_TABLE_NAME)
    print(f"PartitionValues:{SCALING_TEST_PARTITION_VALUES}\n")
    print(f"MaxPositions:{SCALING_TEST_MAX_POSITIONS}\n")
    start = time.time()
    num_partitions=len(SCALING_TEST_PARTITION_VALUES)
    assert number_partitions == num_partitions

    run_scaling_test(SCALING_TEST_STORAGE, SCALING_TEST_PARTITION_VALUES,num_partitions)
    end = time.time()
    print('Launching Latency: %.2f'%(float(start0)-float(launching_time)))
    print("Generating input time %.2f"%(start-start0))
    print('Compaction Time: %.2f'%(end-start))

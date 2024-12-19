## Getting Started

This is a simple sqlite based mock for DeltaCAT storage. Follow below steps to create a new table that can be
consumed by the DeltaCAT compute module. All the APIs expect a kwargs `sqlite3_con` and `sqlite3_cur` to be passed in.
Alternatively, you can also pass `db_file_path` kwarg.

-   Create a virtual env and activate it.

```
python3 -m venv my_env
source my_env/bin/activate
```

-   Open the python interpreter using the command: `PYTHONPATH=$PYTHONPATH:. python3` and then run below to get started.

```python
import sqlite3
import deltacat.tests.local_deltacat_storage as ds

con = sqlite3.connect("deltacat/tests/local_deltacat_storage/db_test.db")
cur = con.cursor()

kwargs = {"sqlite3_con": con, "sqlite3_cur": cur}
result = ds.create_namespace("hash_bucket_test", {}, **kwargs)
# Creating a namespace initializes the catalog tables

list_result = ds.list_namespaces(**kwargs).all_items()

# [{'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'properties': {}}]
```

### Examples

-   Basically, any table you commit to the DeltaCAT storage is persisted into the `.db`. You can choose to commit the `.db` file
    to git repository to simplify your tests.

-   Below are couple of examples of using local DeltaCAT storage APIs.

-   Create a namespace:

```python
ds.create_namespace("hash_bucket_test", {}, **kwargs)

ds.namespace_exists('hash_bucket_test', **kwargs)
# True

ds.get_namespace('hash_bucket_test', **kwargs)
# {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'properties': {}}
```

-   Update a namespace:

```python
ds.update_namespace("hash_bucket_test", {"abcd": "efgh"}, **kwargs)
```

-   Create a table version:

```python
ds.create_table_version('hash_bucket_test', 'test_table', '1', **kwargs)

ds.list_tables('hash_bucket_test', **kwargs).all_items()
# [{'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'description': None, 'properties': None}]

ds.list_table_versions('hash_bucket_test', 'test_table', **kwargs).all_items()
# [{'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': '1'}, 'schema': None, 'partitionKeys': None, 'description': None, 'properties': {'stream_id': '8cf227aa-bc56-4728-8e6a-b993752337c9'}, 'contentTypes': None, 'sort_keys': None}]

ds.table_exists('hash_bucket_test', 'test_table', **kwargs)
# True

ds.table_exists('hash_bucket_test', 'test_table_2', **kwargs)
# False

ds.create_table_version('hash_bucket_test', 'test_table', None, **kwargs) # This will create a new version

ds.get_table_version('hash_bucket_test', 'test_table', '2', **kwargs)
# {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': 2}, 'schema': None, 'partitionKeys': None, 'description': None, 'properties': {'stream_id': '3b4dfe41-0bef-444f-8d7e-51bfe6fa4ec1'}, 'contentTypes': None, 'sort_keys': None}

ds.get_latest_table_version('hash_bucket_test', 'test_table', **kwargs)
# {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': 2}, 'schema': None, 'partitionKeys': None, 'description': None, 'properties': {'stream_id': '3b4dfe41-0bef-444f-8d7e-51bfe6fa4ec1'}, 'contentTypes': None, 'sort_keys': None}

ds.get_table_version_schema('hash_bucket_test', 'test_table', '1', **kwargs) # schema is None

ds.table_version_exists('hash_bucket_test', 'test_table', '3', **kwargs)
# False

ds.get_stream('hash_bucket_test', 'test_table', '2', **kwargs)
# {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': 2}, 'streamId': '3b4dfe41-0bef-444f-8d7e-51bfe6fa4ec1', 'format': 'SQLITE3'}, 'partitionKeys': None, 'state': 'committed', 'previousStreamDigest': None}
```

-   Update a table version

```python

ds.update_table_version('hash_bucket_test', 'test_table_2', '1', **kwargs)

# Traceback (most recent call last):
#   File "<stdin>", line 1, in <module>
#   File "/Users/draghave/deltacat/deltacat/tests/mock_storage/__init__.py", line 326, in update_table_version
#     assert serialized_table_version is not None, \
# AssertionError: Table version not found with locator=592de254fe21cce5331dd80b98c220716d1c48aa|1

ds.update_table_version('hash_bucket_test', 'test_table', '1', **kwargs)
```

-   Update a table

```python
ds.update_table('hash_bucket_test', 'test_table', properties={'acd': 'bcf'}, **kwargs)

ds.get_table('hash_bucket_test', 'test_table', **kwargs)
# {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'properties': {'acd': 'bcf'}, 'description': None}
```

-   Stage and commit a new stream for the table version (notice the stream IDs)

```python
ds.get_table_version('hash_bucket_test', 'test_table', '2', **kwargs)
# {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': 2}, 'schema': None, 'partitionKeys': None, 'description': None, 'properties': {'stream_id': '3b4dfe41-0bef-444f-8d7e-51bfe6fa4ec1'}, 'contentTypes': None, 'sort_keys': None}

staged = ds.stage_stream('hash_bucket_test', 'test_table', '2', **kwargs)
# {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': 2}, 'streamId': '692bf146-e46d-4e8f-adda-f269c0812aac', 'format': 'SQLITE3'}, 'partitionKeys': None, 'state': <CommitState.STAGED: 'staged'>, 'previousStreamDigest': '9ada65a821716a82aae4737ca4b4448b25d42ab4|3b4dfe41-0bef-444f-8d7e-51bfe6fa4ec1|SQLITE3'}

ds.commit_stream(staged, **kwargs)
# {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': 2}, 'streamId': '4e93794c-bac3-47c4-8f51-ef95b68f9d33', 'format': 'SQLITE3'}, 'partitionKeys': None, 'state': <CommitState.COMMITTED: 'committed'>, 'previousStreamDigest': '9ada65a821716a82aae4737ca4b4448b25d42ab4|3b4dfe41-0bef-444f-8d7e-51bfe6fa4ec1|SQLITE3'}

ds.get_table_version('hash_bucket_test', 'test_table', '2', **kwargs)
# {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': 2}, 'schema': None, 'partitionKeys': None, 'description': None, 'properties': {'stream_id': '4e93794c-bac3-47c4-8f51-ef95b68f9d33'}, 'contentTypes': None, 'sort_keys': None}

```

-   Delete a stream along with the table version.

```python
ds.delete_stream('hash_bucket_test', 'test_table', '2', **kwargs)

ds.get_table_version('hash_bucket_test', 'test_table', '2', **kwargs)
# None
```

-   Stage and commit a partition to a stream.

```python
s = ds.get_stream('hash_bucket_test', 'test_table', '1', **kwargs)

ds.get_partition(s.locator, [], **kwargs)
# None

staged = ds.stage_partition(s, [], **kwargs)
# {'partitionLocator': {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': '1'}, 'streamId': '8cf227aa-bc56-4728-8e6a-b993752337c9', 'format': 'SQLITE3'}, 'partitionValues': [], 'partitionId': '3a827bfb-dd8d-4eaa-9570-a2ded3a2bbfe'}, 'schema': None, 'contentTypes': None, 'state': <CommitState.STAGED: 'staged'>, 'previousStreamPosition': None, 'previousPartitionId': None, 'streamPosition': 1691117774995, 'nextPartitionId': None}

ds.commit_partition(staged, **kwargs)
# {'partitionLocator': {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': '1'}, 'streamId': '8cf227aa-bc56-4728-8e6a-b993752337c9', 'format': 'SQLITE3'}, 'partitionValues': [], 'partitionId': '8e0545f1-9d4f-4c72-a5f8-b524bd16dbd7'}, 'schema': None, 'contentTypes': None, 'state': <CommitState.COMMITTED: 'committed'>, 'previousStreamPosition': None, 'previousPartitionId': None, 'streamPosition': 1691118221272, 'nextPartitionId': None}

ds.list_partitions('hash_bucket_test', 'test_table', '1', **kwargs).all_items()
# [{'partitionLocator': {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': '1'}, 'streamId': '8cf227aa-bc56-4728-8e6a-b993752337c9', 'format': 'SQLITE3'}, 'partitionValues': [], 'partitionId': '8e0545f1-9d4f-4c72-a5f8-b524bd16dbd7'}, 'schema': None, 'contentTypes': None, 'state': 'committed', 'previousStreamPosition': None, 'previousPartitionId': None, 'streamPosition': 1691118221272, 'nextPartitionId': None}]

ds.get_partition(s.locator, [], **kwargs)
# {'partitionLocator': {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': '1'}, 'streamId': '8cf227aa-bc56-4728-8e6a-b993752337c9', 'format': 'SQLITE3'}, 'partitionValues': [], 'partitionId': '8e0545f1-9d4f-4c72-a5f8-b524bd16dbd7'}, 'schema': None, 'contentTypes': None, 'state': 'committed', 'previousStreamPosition': None, 'previousPartitionId': None, 'streamPosition': 1691118221272, 'nextPartitionId': None}
```

-   Delete/deprecate a partition.

```python
s = ds.get_stream('hash_bucket_test', 'test_table', '1', **kwargs)

ds.get_partition(s.locator, [], **kwargs)
# {'partitionLocator': {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': '1'}, 'streamId': '8cf227aa-bc56-4728-8e6a-b993752337c9', 'format': 'SQLITE3'}, 'partitionValues': [], 'partitionId': '8e0545f1-9d4f-4c72-a5f8-b524bd16dbd7'}, 'schema': None, 'contentTypes': None, 'state': 'committed', 'previousStreamPosition': None, 'previousPartitionId': None, 'streamPosition': 1691118221272, 'nextPartitionId': None}

ds.delete_partition('hash_bucket_test', 'test_table', '1', [], **kwargs)

ds.get_partition(s.locator, [], **kwargs)
# None

ds.list_partitions('hash_bucket_test', 'test_table', '1', **kwargs).all_items()
# []
```

-   Stage and commit a delta.

```python
import pyarrow as pa
import pandas as pd
from deltacat.storage import Delta

df = pd.DataFrame({'year': [2020, 2022, 2019, 2021],
                   'n_legs': [2, 4, 5, 100],
                   'animals': ["Flamingo", "Horse", "Brittle stars", "Centipede"]})

table = pa.Table.from_pandas(df)

s = ds.get_stream('hash_bucket_test', 'test_table', '1', **kwargs)
staged_partition = ds.stage_partition(s, [], **kwargs)

ds.list_deltas('hash_bucket_test', 'test_table', [], '1', **kwargs).all_items()
# []

delta1 = ds.stage_delta(table, staged_partition, **kwargs) # we stage the same table two times
delta2 = ds.stage_delta(table, staged_partition, **kwargs)

delta1
# {'deltaLocator': {'partitionLocator': {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': '1'}, 'streamId': '8cf227aa-bc56-4728-8e6a-b993752337c9', 'format': 'SQLITE3'}, 'partitionValues': [], 'partitionId': '50b4cace-8aeb-429e-8c8b-b587eb72c610'}, 'streamPosition': 1691182225699}, 'type': <DeltaType.UPSERT: 'upsert'>, 'meta': {'record_count': 4, 'content_length': 1288, 'source_content_length': 1288, 'content_type': <ContentType.PARQUET: 'application/parquet'>, 'content_encoding': <ContentEncoding.IDENTITY: 'identity'>}, 'properties': None, 'manifest': {'id': 'd7371b7b-2903-4056-b574-14285c0e2a7d', 'meta': {'record_count': 4, 'content_length': 1288, 'source_content_length': 1288, 'content_type': <ContentType.PARQUET: 'application/parquet'>, 'content_encoding': <ContentEncoding.IDENTITY: 'identity'>}, 'entries': [{'url': 'cloudpickle://d7371b7b-2903-4056-b574-14285c0e2a7d', 'uri': 'cloudpickle://d7371b7b-2903-4056-b574-14285c0e2a7d', 'meta': {'record_count': 4, 'content_length': 1288, 'source_content_length': 1288, 'content_type': <ContentType.PARQUET: 'application/parquet'>, 'content_encoding': <ContentEncoding.IDENTITY: 'identity'>}, 'mandatory': True, 'id': 'd7371b7b-2903-4056-b574-14285c0e2a7d'}]}, 'previousStreamPosition': 1691182225675}

merged_delta = Delta.merge_deltas([delta1, delta2])

# {'deltaLocator': {'partitionLocator': {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': '1'}, 'streamId': '8cf227aa-bc56-4728-8e6a-b993752337c9', 'format': 'SQLITE3'}, 'partitionValues': [], 'partitionId': '50b4cace-8aeb-429e-8c8b-b587eb72c610'}, 'streamPosition': None}, 'type': <DeltaType.UPSERT: 'upsert'>, 'meta': {'record_count': 8, 'content_length': 2576, 'source_content_length': 2576, 'content_type': <ContentType.PARQUET: 'application/parquet'>, 'content_encoding': <ContentEncoding.IDENTITY: 'identity'>}, 'properties': None, 'manifest': {'id': '60cac30e-b96f-4b4c-9305-e31a63b90424', 'meta': {'record_count': 8, 'content_length': 2576, 'source_content_length': 2576, 'content_type': <ContentType.PARQUET: 'application/parquet'>, 'content_encoding': <ContentEncoding.IDENTITY: 'identity'>}, 'entries': [{'url': 'cloudpickle://d7371b7b-2903-4056-b574-14285c0e2a7d', 'uri': 'cloudpickle://d7371b7b-2903-4056-b574-14285c0e2a7d', 'meta': {'record_count': 4, 'content_length': 1288, 'source_content_length': 1288, 'content_type': <ContentType.PARQUET: 'application/parquet'>, 'content_encoding': <ContentEncoding.IDENTITY: 'identity'>}, 'mandatory': True, 'id': 'd7371b7b-2903-4056-b574-14285c0e2a7d'}, {'url': 'cloudpickle://48d9e010-c87d-4ca5-8f92-375f039a4f6f', 'uri': 'cloudpickle://48d9e010-c87d-4ca5-8f92-375f039a4f6f', 'meta': {'record_count': 4, 'content_length': 1288, 'source_content_length': 1288, 'content_type': <ContentType.PARQUET: 'application/parquet'>, 'content_encoding': <ContentEncoding.IDENTITY: 'identity'>}, 'mandatory': True, 'id': '48d9e010-c87d-4ca5-8f92-375f039a4f6f'}]}, 'previousStreamPosition': 1691182225675}

ds.list_deltas('hash_bucket_test', 'test_table', [], '1', **kwargs).all_items() # deltas aren't available until committed.
# []

committed_delta = ds.commit_delta(merged_delta, **kwargs)

ds.list_deltas('hash_bucket_test', 'test_table', [], '1', **kwargs).all_items() # Still deltas aren't available as partition is not committed.
# []

ds.list_partition_deltas(staged_partition, **kwrgs).all_items() # However, it can be queried via this method

# [{'deltaLocator': {'partitionLocator': {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': '1'}, 'streamId': '8cf227aa-bc56-4728-8e6a-b993752337c9', 'format': 'SQLITE3'}, 'partitionValues': [], 'partitionId': '59432771-32ae-4f0f-b719-1331a605b9f2'}, 'streamPosition': None}, 'type': 'upsert', 'meta': {'record_count': 8, 'content_length': 2576, 'source_content_length': 2576, 'content_type': 'application/parquet', 'content_encoding': 'identity'}, 'properties': None, 'manifest': None, 'previousStreamPosition': 1691182795991}]


ds.commit_partition(staged_partition, **kwargs)

ds.list_deltas('hash_bucket_test', 'test_table', [], '1', **kwargs).all_items()

# [{'deltaLocator': {'partitionLocator': {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': '1'}, 'streamId': '8cf227aa-bc56-4728-8e6a-b993752337c9', 'format': 'SQLITE3'}, 'partitionValues': [], 'partitionId': '59432771-32ae-4f0f-b719-1331a605b9f2'}, 'streamPosition': None}, 'type': 'upsert', 'meta': {'record_count': 8, 'content_length': 2576, 'source_content_length': 2576, 'content_type': 'application/parquet', 'content_encoding': 'identity'}, 'properties': None, 'manifest': None, 'previousStreamPosition': 1691182795991}]

ds.get_latest_delta('hash_bucket_test', 'test_table', [], '1', True, **kwargs)

# {'deltaLocator': {'partitionLocator': {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': '1'}, 'streamId': '8cf227aa-bc56-4728-8e6a-b993752337c9', 'format': 'SQLITE3'}, 'partitionValues': [], 'partitionId': '59432771-32ae-4f0f-b719-1331a605b9f2'}, 'streamPosition': None}, 'type': 'upsert', 'meta': {'record_count': 8, 'content_length': 2576, 'source_content_length': 2576, 'content_type': 'application/parquet', 'content_encoding': 'identity'}, 'properties': None, 'manifest': {'id': 'a2c14adb-7ac6-46e9-b5ce-7f9fb768dd48', 'meta': {'record_count': 8, 'content_length': 2576, 'source_content_length': 2576, 'content_type': 'application/parquet', 'content_encoding': 'identity'}, 'entries': [{'url': 'cloudpickle://2f2f7bbe-e6fc-4c93-a445-03b41af6c10c', 'uri': 'cloudpickle://2f2f7bbe-e6fc-4c93-a445-03b41af6c10c', 'meta': {'record_count': 4, 'content_length': 1288, 'source_content_length': 1288, 'content_type': 'application/parquet', 'content_encoding': 'identity'}, 'mandatory': True, 'id': '2f2f7bbe-e6fc-4c93-a445-03b41af6c10c'}, {'url': 'cloudpickle://f64d3c8a-2cd7-4e75-aee3-9fa12fa1e6a3', 'uri': 'cloudpickle://f64d3c8a-2cd7-4e75-aee3-9fa12fa1e6a3', 'meta': {'record_count': 4, 'content_length': 1288, 'source_content_length': 1288, 'content_type': 'application/parquet', 'content_encoding': 'identity'}, 'mandatory': True, 'id': 'f64d3c8a-2cd7-4e75-aee3-9fa12fa1e6a3'}]}, 'previousStreamPosition': 1691182795991}

ds.get_delta('hash_bucket_test', 'test_table', 1691182796009, [], '1', True, **kwargs) # Get delta1 using it's stream position

# {'deltaLocator': {'partitionLocator': {'streamLocator': {'tableVersionLocator': {'tableLocator': {'namespaceLocator': {'namespace': 'hash_bucket_test'}, 'tableName': 'test_table'}, 'tableVersion': '1'}, 'streamId': '8cf227aa-bc56-4728-8e6a-b993752337c9', 'format': 'SQLITE3'}, 'partitionValues': [], 'partitionId': '59432771-32ae-4f0f-b719-1331a605b9f2'}, 'streamPosition': 1691182796009}, 'type': 'upsert', 'meta': {'record_count': 4, 'content_length': 1288, 'source_content_length': 1288, 'content_type': 'application/parquet', 'content_encoding': 'identity'}, 'properties': None, 'manifest': {'id': '2f2f7bbe-e6fc-4c93-a445-03b41af6c10c', 'meta': {'record_count': 4, 'content_length': 1288, 'source_content_length': 1288, 'content_type': 'application/parquet', 'content_encoding': 'identity'}, 'entries': [{'url': 'cloudpickle://2f2f7bbe-e6fc-4c93-a445-03b41af6c10c', 'uri': 'cloudpickle://2f2f7bbe-e6fc-4c93-a445-03b41af6c10c', 'meta': {'record_count': 4, 'content_length': 1288, 'source_content_length': 1288, 'content_type': 'application/parquet', 'content_encoding': 'identity'}, 'mandatory': True, 'id': '2f2f7bbe-e6fc-4c93-a445-03b41af6c10c'}]}, 'previousStreamPosition': 1691182795991}
```

-   Downloading the data from the catalog.

```python
import pyarrow as pa
from deltacat.types.media import StorageType

tables = ds.download_delta(committed_delta, storage_type=StorageType.LOCAL **kwargs)

assert len(tables) == 2

pa.concat_tables(tables)

# pyarrow.Table
# year: int64
# n_legs: int64
# animals: string
# ----
# year: [[2020,2022,2019,2021],[2020,2022,2019,2021]]
# n_legs: [[2,4,5,100],[2,4,5,100]]
# animals: [["Flamingo","Horse","Brittle stars","Centipede"],["Flamingo","Horse","Brittle stars","Centipede"]]
```

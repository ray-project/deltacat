# DeltaCAT Compactor

The DeltaCAT compactor provides a fast, scalable, and efficient
Log-Structure-Merge (LSM) based Change-Data-Capture (CDC) implementation using
Ray. It implements [The Flash Compactor Design](TheFlashCompactorDesign.pdf)
using DeltaCAT's portable delta catalog storage APIs.

The compactor is filesystem-agnostic and works with any valid PyArrow filesystem
or fsspec filesystem through the DeltaCAT catalog abstraction.

## User Guide
### Migrating to the DeltaCAT Compactor
Migration from your old copy-on-write CDC framework to DeltaCAT is typically
done by first running a rebase on top of your old copy-on-write compacted
results.

A _rebase_ allows you to run compaction from source **S1** to destination **D**
on behalf of source **S2**, where **S1** and **S2** can either be the same or
different tables. More specifically, it:
1. Discards (does not read) any prior compatible round completion info and primary key indices associated with **S1**/**S2** and **D**.
2. Writes a round completion file associated with **S2** and **D** (including a primary key index for **D** and an optional rebased high watermark stream position for **S1**).
3. Saves and propagates the last-used rebase source across all subsequent round completion files.

As part of a rebase from an alternate source or as an independent operation,
you can optionally set a rebase source high watermark stream position that will
be used as the starting stream position (exclusive) for the next compaction
round.

For example, a table rebase can be used to more easily transition from an old
copy-on-write compactor to the DeltaCAT compactor by rebasing on top of the
results of the old copy-on-write compactor.

If we assume `delta_source` refers to the table that both the old compactor and
the new compactor will read and merge deltas from, then your first call to
`compact_partition` should look something like this:
```python
from deltacat.compute.compactor.compaction_session import compact_partition
from deltacat.catalog.model.catalog_properties import CatalogProperties

# Create a catalog that points to your storage location
catalog = CatalogProperties.of({'root': 's3://your-bucket'})

compact_partition(
  source_partition_locator=old_compacted_partition,  # S1
  destination_partition_locator=deltacat_compacted_partition,  # D
  primary_keys=delta_source_primary_keys,
  catalog=catalog,
  last_stream_position_to_compact=delta_source_last_stream_position,
  rebase_source_partition_locator=delta_source_partition,  # S2
  rebase_source_partition_high_watermark=delta_source_last_compacted_stream_position,
)
```

Note that, in the above call, `delta_source_last_compacted_stream_position`
refers to the last stream position compacted into `old_compacted_partition`.

Then, to compact subsequent incremental deltas from `delta_source` on top of
`deltacat_compacted_partition`, you simply set `source_partition_locator` to
the last rebase source:
```python
from deltacat.compute.compactor.compaction_session import compact_partition

compact_partition(
  source_partition_locator=delta_source_partition,  # S2
  destination_partition_locator=deltacat_compacted_partition,  # D
  primary_keys=delta_source_primary_keys,
  catalog=catalog,  # filesystem-agnostic catalog
  last_stream_position_to_compact=delta_source_last_stream_position,
)
```

The first call will run an incremental compaction from
`rebase_source_partition_high_watermark` (exclusive) to
`last_stream_position_to_compact` (inclusive) while re-using the round
completion file and primary key index created during the rebased compaction.
All subsequent incremental compactions can be run the same way, and will
continue compacting from the old last stream position to compact up to the new
last stream position to compact while re-using the last compaction round's
round completion file and primary key index.

### Discarding Cached Compaction Results
Another use-case for a compaction rebase is to ignore and overwrite any cached
results persisted from prior compaction job runs, perhaps because 1 or more
cached files were corrupted, or for testing purposes. In this case, simply set
`source_partition_locator` and `rebase_source_partition_locator` to the same
value:
```python
from deltacat.compute.compactor.compaction_session import compact_partition

compact_partition(
  source_partition_locator=source_partition_to_compact,
  destination_partition_locator=deltacat_compacted_partition,
  primary_keys=delta_source_primary_keys,
  catalog=catalog,
  last_stream_position_to_compact=delta_source_last_stream_position,
  rebase_source_partition_locator=source_partition_to_compact,
)
```

This will ignore any existing round completion file or primary key index
previously produced by prior compaction rounds, and force a backfill compaction
job to run from the first delta stream position in `source_partition_locator`
up to `last_stream_position_to_compact` (inclusive).

All subsequent incremental compactions can now run as usual by simply omitting
`rebase_source_partition_locator`:
```python
from deltacat.compute.compactor.compaction_session import compact_partition

compact_partition(
    source_partition_locator=source_partition_to_compact,
    destination_partition_locator=deltacat_compacted_partition,
    primary_keys=delta_source_primary_keys,
    catalog=catalog,
    last_stream_position_to_compact=delta_source_last_stream_position,
)
```

This will re-use the round completion file and primary key index produced by
the last compaction round, and compact all source partition deltas between
the prior invocation's `last_stream_position_to_compact` (exclusive) and this
invocation's `last_stream_position_to_compact` (inclusive).

### Catalog Configuration

The compactor uses a catalog abstraction that works with any PyArrow or fsspec filesystem:

```python
from deltacat.catalog.model.catalog_properties import CatalogProperties

# S3 filesystem
s3_catalog = CatalogProperties.of({'root': 's3://my-bucket/path'})

# Google Cloud Storage
gcs_catalog = CatalogProperties.of({'root': 'gs://my-bucket/path'})

# Azure Blob Storage
azure_catalog = CatalogProperties.of({'root': 'az://my-container/path'})

# Local filesystem
local_catalog = CatalogProperties.of({'root': '/path/to/local/storage'})

# HDFS
hdfs_catalog = CatalogProperties.of({'root': 'hdfs://namenode/path'})
```

The compactor will automatically use the appropriate filesystem driver based on the URL scheme.

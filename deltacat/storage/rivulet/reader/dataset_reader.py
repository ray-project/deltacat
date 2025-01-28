import logging
from typing import Generator, Optional, Set, Type, TypeVar, Any

from deltacat.storage.model.shard import Shard
from deltacat.storage.rivulet.metastore.sst import SSTableRow, SSTable
from deltacat.storage.rivulet.metastore.sst_interval_tree import (
    BlockIntervalTree,
    OrderedBlockGroups,
)
from deltacat.storage.rivulet.reader.block_scanner import BlockScanner
from deltacat.storage.rivulet.reader.dataset_metastore import (
    DatasetMetastore,
    ManifestAccessor,
)
from deltacat.storage.rivulet.reader.query_expression import QueryExpression
from deltacat.storage.rivulet import Schema

# The type of data returned to reader
T = TypeVar("T")


class DatasetReader:
    """
    DatasetReader is an internal class used to execute a scan

    TODO - Currently, this reader is limited to reading a single field group
       The next CR will fast follow to modify this to read and zipper multiple field groups

    TODO currently this assumes all SST files are L0 files with overlapping key ranges
        Future CR will support L1+ SSTs
    """

    BLOCK_READER_POOL_SIZE = 8

    def __init__(self, metastore: DatasetMetastore):
        self.metastore: DatasetMetastore = metastore
        self.block_scanner = BlockScanner(self.metastore)

    def scan(
        self,
        schema: Schema,
        deserialize_to: Type[T],
        query: QueryExpression[Any](),
        shard: Optional[Shard] = None,
    ) -> Generator[T, None, None]:
        """
        Scan records given query and deserialize to desired memory output format

        # TODO handle "partial schema" use case, in which the query schema is a subset of full schema

        # TODO this is where we will do the ziper merge when we support multiple field groups
        # for each SST row which may overlap key range, read data chunk
        # we will later improve and parallelize this when we do zipper merge work
        """

        # Read manifests and differentiate between "full schema" and "zipper merge" use case
        manifests = set(self.metastore.generate_manifests())
        schemas = set([manifest.context.schema for manifest in manifests])
        levels = set([manifest.context.level for manifest in manifests])
        # Must zipper if there are multiple schemas
        cannot_avoid_zipper = len(schemas) > 1
        # Must zipper if L0 is involved or if manifests span multiple levels
        cannot_avoid_zipper |= 0 in levels or len(levels) > 0

        if cannot_avoid_zipper:
            logging.info(f"Done scanning manifests. Can avoid zipper-merge")
            for scan_result in self.__scan_with_zipper(
                schema, deserialize_to, manifests, query, shard=shard
            ):
                yield scan_result
        else:
            logging.info(f"Done scanning manifests. Must perform zipper-merge")
            for scan_result in self.__scan_no_zipper(
                schema, deserialize_to, manifests, query, shard=shard
            ):
                yield scan_result

    def __scan_no_zipper(
        self,
        schema: Schema,
        deserialize_to: Type[T],
        manifests: Set[ManifestAccessor],
        query: QueryExpression[Any](),
        shard: Optional[Shard] = None,
    ) -> Generator[T, None, None]:
        # Build final query using user query and shard boundaries (ensures only blocks in shard and query range are read).
        # TODO: improve query expression implementation to have a builder of some sort.
        query = QueryExpression().with_shard(query, shard)
        # Map manifests to all SST rows which match query
        matching_sst_rows: Set[SSTableRow] = {
            row
            for manifest in manifests
            for table in manifest.generate_sstables()
            for row in self.__load_sst_rows(table, query)
        }

        for result_row in self.block_scanner.scan(
            schema, deserialize_to, matching_sst_rows, query
        ):
            yield result_row

    def __scan_with_zipper(
        self,
        schema: Schema,
        deserialize_to: Type[T],
        manifests: Set[ManifestAccessor],
        query: QueryExpression[Any](),
        shard: Optional[Shard] = None,
    ) -> Generator[T, None, None]:
        # Build final query using user query and shard boundaries (ensures only blocks in shard and query range are read).
        # TODO: improve query expression implementation to have a builder of some sort.
        query = QueryExpression().with_shard(query, shard)
        # Build interval tree from manifests and plan scan
        sst_interval_tree = BlockIntervalTree()
        for manifest in manifests:
            for table in manifest.generate_sstables():
                rows = self.__load_sst_rows(table, query)
                sst_interval_tree.add_sst_rows(rows, manifest.context)

        scan_block_groups: OrderedBlockGroups = (
            sst_interval_tree.get_sorted_block_groups(query.min_key, query.max_key)
        )
        for result_row in self.block_scanner.scan_with_zipper(
            schema, deserialize_to, scan_block_groups, query
        ):
            yield result_row

    def __load_sst_rows(
        self, table: SSTable, query: QueryExpression
    ) -> Set[SSTableRow]:
        # Short circuit table if there isn't any overlap with min and max
        if not self.__overlaps_primary_key_range(query, table.min_key, table.max_key):
            return set()
        return {
            r
            for r in table.rows
            if self.__overlaps_primary_key_range(query, r.key_min, r.key_max)
        }

    def __overlaps_primary_key_range(
        self, query: QueryExpression, min_key, max_key
    ) -> bool:
        """
        Helper method to check whether a query expression has overlap with a primary key range
        """
        # If no PK range set, the query is across all primary keys, so return true
        if not query.key_range:
            return True

        query_start, query_end = query.key_range
        if query_end < min_key:
            return False
        elif query_start > max_key:
            return False
        else:
            return True

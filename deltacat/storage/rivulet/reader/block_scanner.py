import heapq
import logging

from collections import defaultdict
from typing import (
    Generator,
    Dict,
    Set,
    Type,
    TypeVar,
    NamedTuple,
    Any,
    List,
    Generic,
    AbstractSet,
)

from deltacat.storage.rivulet.metastore.delta import DeltaContext
from deltacat.storage.rivulet.metastore.sst import SSTableRow
from deltacat.storage.rivulet.metastore.sst_interval_tree import (
    OrderedBlockGroups,
    BlockGroup,
    Block,
)
from deltacat.storage.rivulet.reader.data_reader import RowAndKey, FileReader
from deltacat.storage.rivulet.reader.dataset_metastore import DatasetMetastore
from deltacat.storage.rivulet.reader.pyarrow_data_reader import ArrowDataReader
from deltacat.storage.rivulet.reader.query_expression import QueryExpression
from deltacat.storage.rivulet.reader.reader_type_registrar import FileReaderRegistrar
from deltacat.storage.rivulet import Schema
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

FILE_FORMAT = TypeVar("FILE_FORMAT")
MEMORY_FORMAT = TypeVar("MEMORY_FORMAT")


class FileReaderWithContext(NamedTuple):
    reader: FileReader[FILE_FORMAT]
    context: DeltaContext


class ZipperMergeHeapRecord(NamedTuple):
    """
    Named tuple for data structure we're putting into heap during zipper merge

    Note we override the equality/comparison operators to use key
        so that we can add these items to a heap by key
    """

    key: Any
    data: FILE_FORMAT
    reader: FileReaderWithContext

    def __lt__(self, other):
        return self.key < other.key

    def __le__(self, other):
        return self.key <= other.key

    def __gt__(self, other):
        return self.key > other.key

    def __ge__(self, other):
        return self.key >= other.key


class ZipperBlockScanExecutor(Generic[MEMORY_FORMAT]):
    """
    Class for managing a zipper scan across multiple field groups. This class is only ever called inside the higher level BlockScanner class

    It is factored into a dedicated class because of the complexity and state management of
     zipper merging
    """

    def __init__(
        self,
        result_schema: Schema,
        deserialize_to: Type[MEMORY_FORMAT],
        ordered_block_groups: OrderedBlockGroups,
        query: QueryExpression[Any],
        metastore: DatasetMetastore,
        file_readers: Dict[str, FileReader],
    ):

        self.result_schema = result_schema
        self.deserialize_to = deserialize_to
        self.ordered_block_groups = ordered_block_groups
        self.query = query
        self.metastore = metastore
        self.file_readers = file_readers
        """
        Keeps track of block file readers that are open, across block group boundaries. E.g., if Block Group 1 has
        blocks [1,2,3] and BlockGroup2 has blocks [2,3], we will start reading blocks [2,3] and need to re-use the
        open iterator while reading BlockGroup2
        """
        self._open_file_readers: Dict[SSTableRow, FileReaderWithContext] = {}

    def scan(self) -> Generator[MEMORY_FORMAT, None, None]:
        """
        Perform N-wise zipper across N field groups.
        Within each field group, there is a set of blocks which belong in this BlockGroup's key range

        As a simplified example, we may have:
        FieldGroup1: [BlockA, BlockB]
        FieldGroup2: [BlockC]
        BlockA: keys 1,3,9,10
        BlockB: keys 2,4,5,6,7,8
        BlockC: keys 1-10

        The algorithm to merge these looks like:
        1. Load each block in DataReader to get iterator over sorted keys
        2. Build a heap of records across blocks across field groups
        3. Pop record(s) from heap as long as they have equal keys. For up to N records, merge column wise
        4. Continue until all blocks are read OR the key range in query is exceeded
        """
        for block_group in self.ordered_block_groups.block_groups:

            logger.debug(f"Starting scan of block group {block_group}")

            # Set of all blocks that need to be read within this block group
            blocks: set[Block] = {
                block
                for block_set in block_group.field_group_to_blocks.values()
                for block in block_set
            }
            # Open all file readers, such that self._open_block_iterators has pointers to open readers
            self.__open_file_readers(blocks)
            record_heap: List[ZipperMergeHeapRecord] = []

            # Seed record heap with record from each iterator
            file_reader_context: FileReaderWithContext
            for block, file_reader_context in self._open_file_readers.items():
                self.__push_next_row_back_to_heap(
                    block_group, file_reader_context, record_heap
                )

            # For each zipper merged entry from heap traversal, delegate to deserializer
            for zipper_merged in self.__zipper_merge_sorted_records(
                record_heap, block_group
            ):
                records = [z.data for z in self._dedupe_records(zipper_merged)]
                # TODO (multi format support) we need to handle joining across data readers in the future
                # For now, assume all data readers MUST read to Arrow intermediate format
                for result in ArrowDataReader().join_deserialize_records(
                    records, self.deserialize_to, self.result_schema.get_merge_key()
                ):
                    yield result

    def _dedupe_records(
        self, records: List[ZipperMergeHeapRecord]
    ) -> List[ZipperMergeHeapRecord]:
        """Deduplicate records with the same key (as a sorted list of records).

        Deduplication chooses records based on the following rules of precedence

        1. Levels with lower numbers take precedence over levels with higher numbers (L0 is preferred over L1)
        2. Newer stream positions take precedence over older stream positions

        Undefined Behavior:

        - Duplicate records within files from the same manifest (either in the same ir across data files)

        TODO: allow for the definition of a 'dedupe' column to break ties.
        """
        sort_criteria = lambda x: (
            -x.reader.context.level,
            x.reader.context.stream_position,
        )

        grouped_by_sort_group: defaultdict[
            Schema, List[ZipperMergeHeapRecord]
        ] = defaultdict(list)
        for record in records:
            grouped_by_sort_group[record.reader.context.schema].append(record)
        deduped = [
            max(group, key=sort_criteria) for group in grouped_by_sort_group.values()
        ]
        # Sort one last time across schemas (in case there's overlapping fields)
        deduped.sort(key=sort_criteria)
        return deduped

    def __zipper_merge_sorted_records(
        self, record_heap: List[ZipperMergeHeapRecord], block_group: BlockGroup
    ) -> Generator[List[ZipperMergeHeapRecord], None, None]:
        """
        Continually pop from heap until heap empty OR block range exceeded. Generate "zipper merge" of records

        Algorithm is:
        (1) Pop lowest element from heap. Includes pointer to the iterator it came from.
                Push next largest element from that generator back onto heap
        (2) Buffer records of same key and peek/pop the heap as long as there is a key match
                For any record popped, push next largest element from generator back onto heap
        (3) Yield merged record by invoking Data Reader

        This solution maintains the following invariants:
        (1) the heap will have at most N records, where N=total blocks in BlockGroup
        (2) the heap has the N smallest records globally
        (3) any data that needs to be merged for a given key exists in the heap

        :param record_heap: seeded heap of ZipperMergeHeapRecords.
        :param block_group: block group being traversed
        :return: generator of merged records. Note this is a list not a set to not require hash support
        """
        if not record_heap:
            return

        # Keep iterating until heap is empty or key range is exceeded
        while record_heap:
            curr_heap_record = heapq.heappop(record_heap)
            curr_pk = curr_heap_record.key

            if not self.query.matches_query(curr_pk):
                continue

            # Sanity check - assert that key we are looking at is in block group's range
            if not block_group.key_in_range(curr_pk):
                raise RuntimeError(
                    f"Did not expect to find key {curr_pk} on zipper merge heap"
                    f"for block group {block_group}"
                )

            # Find all records to be merged by continuing to pop heap
            merged_by_pk = [curr_heap_record]
            # For the current record itself - push next row back to heap
            self.__push_next_row_back_to_heap(
                block_group, curr_heap_record.reader, record_heap
            )
            # For the rest of the heap elements - peek/pop as long as they equal key
            # Note that heap[0] is equivalent to peek operation
            while record_heap and record_heap[0][0] == curr_pk:
                merge_heap_record: ZipperMergeHeapRecord = heapq.heappop(record_heap)
                merged_by_pk.append(merge_heap_record)
                self.__push_next_row_back_to_heap(
                    block_group, merge_heap_record.reader, record_heap
                )
            yield merged_by_pk

    def __push_next_row_back_to_heap(
        self,
        block_group: BlockGroup,
        row_context: FileReaderWithContext,
        record_heap: List[ZipperMergeHeapRecord],
    ):
        """
        This is a helper function for __zipper_merge_sorted_records and for scan().

        Given a file reader, it will next() records until it finds the next record within the block group
        and current query. It then pushes that record onto the heap

        Sometimes we end up needing to seek into the middle of a block because the key range of a query starts
        in the middle of the block. For example, if the block has keys range [0,100],
        and the query is for keys [50-100], we need to seek to the first key in the block that is >= 50

        TODO better support for seeking within block (rather than O(N) iteration)
        """

        file_reader = row_context.reader
        while file_reader.peek() is not None and (
            block_group.key_below_range(file_reader.peek().key)
            or self.query.below_query_range(file_reader.peek().key)
        ):
            try:
                # call next() on file reader to throw out key which is below range of block group
                next(file_reader)
            except StopIteration:
                # If we have exhausted iterator, this just means no keys from this block actually match the query
                file_reader.close()
                # TODO how to remove file reader from _open_file_readers?

        if (
            file_reader.peek()
            and self.query.matches_query(file_reader.peek().key)
            and block_group.key_in_range(file_reader.peek().key)
        ):
            try:
                r: RowAndKey = next(file_reader)
                heapq.heappush(
                    record_heap,
                    ZipperMergeHeapRecord(r.key, r.row, row_context),
                )
            except StopIteration:
                # This means we have exhausted the open FileReader and should close it
                file_reader.__exit__()
                # TODO how to remove file reader from _open_file_readers?

    def __open_file_readers(self, blocks: AbstractSet[Block]):
        """
        This method should be called once per block group.
        It opens iterators across all blocks in the block group and stores them in a map
        Blocks may already be open, if they were also in previous block groups.
        """
        for block in blocks:
            sst_row: SSTableRow = block.row
            if sst_row not in self._open_file_readers:
                file_reader = FileReaderRegistrar.construct_reader_instance(
                    sst_row,
                    self.metastore.file_provider,
                    self.result_schema.get_merge_key(),
                    self.result_schema,
                    self.file_readers,
                )
                file_reader.__enter__()
                # TODO we need some way to compare the blocks. using serialized timestamp as proxy for now
                context = FileReaderWithContext(file_reader, block.context)
                self._open_file_readers[sst_row] = context


class BlockScanner:
    """
    BlockScanner is a low level internal class which performs IO on Block Groups

    Note that we expect a block scanner to be initialized PER QUERY because it will keep state about ongoing execution,
      e.g. open iterators across block groups

    TODO efficiency improvements like parallelizing scanning.
    TODO handle "partial schema" use case, in which the query schema is a subset of full schema
    TODO in the future we will probably want to cache blocks read across queries
    """

    def __init__(self, metastore: DatasetMetastore):
        # Persist initialized file readers
        self.metastore = metastore
        self.file_readers: Dict[str, FileReader] = {}

    def scan(
        self,
        schema: Schema,
        deserialize_to: Type[MEMORY_FORMAT],
        blocks: Set[SSTableRow],
        query: QueryExpression[Any](),
    ) -> Generator[MEMORY_FORMAT, None, None]:
        """
        Scan records given query and deserialize to desired memory output format
        Set of blocks can all be scanned and returned independently
        TODO handle "partial schema" use case, in which the query schema is a subset of full schema
        TODO parallelize scan with async io
        """
        data_reader = ArrowDataReader()
        for block in blocks:
            file_reader = FileReaderRegistrar.construct_reader_instance(
                block,
                self.metastore.file_provider,
                schema.get_merge_key(),
                schema,
                self.file_readers,
            )
            with file_reader:
                for generated_records in file_reader.__iter__():
                    # Check whether row matches key in query before deserializing
                    if query.key_range:
                        start, end = query.key_range
                        if generated_records.key < start or generated_records.key > end:
                            continue

                    # Otherwise, key predicate matched and yield deserialized row
                    for deserialized_row in data_reader.deserialize_records(
                        generated_records, deserialize_to
                    ):
                        yield deserialized_row

    def scan_with_zipper(
        self,
        schema: Schema,
        deserialize_to: Type[MEMORY_FORMAT],
        ordered_block_groups: OrderedBlockGroups,
        query: QueryExpression[Any](),
    ) -> Generator[MEMORY_FORMAT, None, None]:
        zipper_scan_executor = ZipperBlockScanExecutor(
            schema,
            deserialize_to,
            ordered_block_groups,
            query,
            self.metastore,
            self.file_readers,
        )
        return zipper_scan_executor.scan()

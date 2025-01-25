from typing import Generator, Dict, Optional

import pyarrow as pa

from deltacat.storage.model.shard import Shard
from deltacat.storage.rivulet.reader.dataset_reader import DatasetReader
from deltacat.storage.rivulet.reader.query_expression import QueryExpression
from deltacat.storage.rivulet import Schema


class DataScan:
    """
    Top level class representing and executing a data scan, on both riv internal and external data
    This class is lazy, and executed when the user calls a method "to_{format}"
    to deserialize data into the chosen in-memory format

    Dataset.py scan() is the entrypoint to create and return data scan. The user
    then has to chain a "to_{format}" method to read rows in their chosen in-memory format

    Rivulet cannot simply return file URIs and allow query engine to process files,
    because rivulet will internally manage details like indexes, custom file formats for bulk records, where data is physically laid out across row groups, etc.

    DataScan allows query engines to send push down predicates. Push down predicates are used to filter on dimensions natively indexed by riv (e.g. primary key), and also

    DataScan is coupled to internals of the riv rivulet format. If the rivulet format evolves, DataScan execution hould be able to understand which rivulet spec version is used and be compatible with any valid rivule rivulet.

    FUTURE IMPROVEMENTS
    1. Implement full spec for push down predicates
    2. Figure out how permissions/credential providers work.
    3. Figure out how extension libraries can plug in to_x deserialization support.
        One potential option is to override __getattr__ and check a static class-level Registry
        of to_x methods. Modules would have to import DataScan and call DataScan.register_deserializer(...)
    """

    def __init__(
        self,
        dataset_schema: Schema,
        query: QueryExpression,
        dataset_reader: DatasetReader,
        shard: Optional[Shard],
    ):
        self.dataset_schema = dataset_schema
        self.query = query
        self.dataset_reader = dataset_reader
        self.shard = shard

    def to_arrow(self) -> Generator[pa.RecordBatch, None, None]:
        """
        Generates scan results as arrow record batches

        TODO how to make the .to_x methods pluggable?
        """
        return self.dataset_reader.scan(
            self.dataset_schema, pa.RecordBatch, self.query, shard=self.shard
        )

    def to_pydict(self) -> Generator[Dict, None, None]:
        """
        Generates scan results as a Dict for each row
        """
        return self.dataset_reader.scan(
            self.dataset_schema, Dict, self.query, shard=self.shard
        )

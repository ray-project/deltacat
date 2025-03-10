import logging
import json
import pyarrow as pa
import pyarrow.parquet
import pyarrow.feather
from typing import Callable, Dict

from deltacat.storage.rivulet.reader.query_expression import QueryExpression
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def export_parquet(dataset, file_uri: str, query: QueryExpression = QueryExpression()):
    records = dataset.scan(query).to_arrow()
    table = pa.Table.from_batches(records)
    pyarrow.parquet.write_table(table, file_uri)


def export_feather(dataset, file_uri: str, query: QueryExpression = QueryExpression()):
    records = dataset.scan(query).to_arrow()
    table = pa.Table.from_batches(records)
    pyarrow.feather.write_feather(table, file_uri)


def export_json(dataset, file_uri: str, query: QueryExpression = QueryExpression()):
    with open(file_uri, "w") as f:
        for batch in dataset.scan(query).to_pydict():
            json.dump(batch, f, indent=2)
            f.write("\n")


def export_dataset(dataset, file_uri: str, format: str = "parquet", query=None):
    """
    Export the dataset to a file.

    TODO: Make this pluggable for custom formats.

    Args:
        dataset: The dataset to export.
        file_uri: The URI to write the dataset to.
        format: The format to write the dataset in. Options are [parquet, feather, json].
        query: QueryExpression to filter the dataset before exporting.
    """
    # Supported format handlers
    export_handlers: Dict[str, Callable] = {
        "parquet": export_parquet,
        "feather": export_feather,
        "json": export_json,
    }

    if format not in export_handlers:
        raise ValueError(
            f"Unsupported format: {format}. Supported formats are {list(export_handlers.keys())}"
        )

    export_handlers[format](dataset, file_uri, query or QueryExpression())

    logger.info(f"Dataset exported to {file_uri} in {format} format.")

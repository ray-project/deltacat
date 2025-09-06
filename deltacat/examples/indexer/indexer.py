import argparse

from datetime import datetime

import ray

import deltacat
import daft
import pyarrow as pa
import pandas as pd
import polars as pl
import numpy as np

from deltacat import DeltaCatUrl


def print_package_version_info() -> None:
    print(f"DeltaCAT Version: {deltacat.__version__}")
    print(f"Ray Version: {ray.__version__}")
    print(f"Daft Version: {daft.__version__}")
    print(f"NumPy Version: {np.__version__}")
    print(f"PyArrow Version: {pa.__version__}")
    print(f"Polars Version: {pl.__version__}")
    print(f"Pandas Version: {pd.__version__}")


def json_path_to_regex(path: str):
    if not path:
        raise ValueError("Path cannot be empty")
    parts = path.split("/")
    leaf_key = parts.pop()
    regex = r""
    for part in parts:
        if part.strip():  # discard leading and/or redundant separators
            regex += rf'"{part}"\s*:\s*[{{\[].*?'
    regex += rf'"{leaf_key}"\s*:\s*"(?<{leaf_key}>.*?)"'
    return regex


def run(
    source: str,
    dest: str,
) -> None:
    # print package version info
    print_package_version_info()

    # run a synchronous copy from the source to the destination
    deltacat.copy(
        DeltaCatUrl(source),
        DeltaCatUrl(dest),
        # reader arguments to pass to the default reader (polars)
        # for the given text-based datasource, it accepts the same
        # arguments as polars.read_csv except for `source`, `n_threads`
        # `new_columns`, `separator`, `has_header`, `quote_char`, and
        # `infer_schema`.
        reader_args={
            "low_memory": True,  # try to use less memory (++stability, --perf)
            "batch_size": 1024,  # text line count read into a buffer at once
            "use_pyarrow": True,  # use the native pyarrow reader
        },
        # writer arguments to pass to the default writer (polars)
        # for the given parquet-based datasink, it generally accepts the same
        # arguments as polars.DataFrame.write_{dest-type} except for `file`
        writer_args={
            "compression": "lz4",  # faster compression & decompression
            # "compression": "zstd",  # better compression ratio
            # "compression": "snappy",  # compatible w/ older Parquet readers
        },
        # Transforms to run against the default polars dataframe read.
        # By default, each transform takes a polars dataframe `df` as input
        # and produces a polars dataframe as output. All transforms listed
        # are run in order (i.e., the dataframe output from transform[0]
        # is the dataframe input to transform[1]).
        #
        # See:
        # https://docs.pola.rs/api/python/stable/reference/dataframe/index.html
        # https://docs.pola.rs/api/python/stable/reference/expressions/index.html
        transforms=[
            lambda df, src: df.rename(
                {"text": "utf8_body"},
            ),
            lambda df, src: df.with_columns(
                pl.col("utf8_body").hash().alias("utf8_body_hash"),
                pl.lit(datetime.utcnow()).dt.datetime().alias("processing_time"),
                pl.lit(src.url_path).alias("source_file_path"),
            ),
        ],
    )


if __name__ == "__main__":
    """
    This example script demonstrates how to use the `deltacat.copy` API to copy multimodal source files into
    arbitrary destinations with optional file format conversion and UDF transformations using DeltaCAT URLs.

    Example 1: Run this script locally using Ray:
    $ python indexer.py \
    $   --source 'text+s3://openalex-mag-format/data_dump_v1/2022-07-08/nlp/PaperAbstractsInvertedIndex.txt_part31' \
    $   --dest 'parquet+s3://deltacat-example-output/openalex/PaperAbstractsInvertedIndex.part31.parquet'

    Example 2: Submit this script as a local Ray job using a local job client:
    >>> from deltacat import local_job_client
    >>> client = local_job_client()
    >>> # read the source file as line-delimited text
    >>> src = "text+s3://openalex-mag-format/data_dump_v1/2022-07-08/nlp/PaperAbstractsInvertedIndex.txt_part31"
    >>> # write to the destination file using the default DeltaCAT Parquet writer (i.e., polars.DataFrame.write_parquet)
    >>> dst = "parquet+s3://deltacat-example-output/openalex/PaperAbstractsInvertedIndex.part31.parquet"
    >>> try:
    >>>   job_run_result = client.run_job(
    >>>       # Entrypoint shell command to run the indexer job
    >>>       entrypoint=f"python indexer.py --source '{src}' --dest '{dst}'",
    >>>       # Path to the local directory that contains the indexer.py file
    >>>       runtime_env={"working_dir": "./deltacat/examples/indexer.py"},
    >>>   )
    >>>   print(f"Job ID {job_run_result.job_id} terminal state: {job_run_result.job_status}")
    >>>   print(f"Job ID {job_run_result.job_id} logs: ")
    >>>   print(job_run_result.job_logs)
    >>> except RuntimeError as e:
    >>>     print(f"Job Run Failed: {e}")
    >>> except TimeoutError as e:
    >>>     print(f"Job Run Timed Out: {e}")

    Example 3: Submit this script as a remote Ray job using a remote job client:
    >>> from deltacat import job_client
    >>> # use `deltacat.yaml` from the current working directory as the ray cluster launcher config file
    >>> # automatically launches the cluster if it doesn't exist or has died
    >>> # automatically forwards the ray cluster's dashboard for viewing in a web browser @ http://localhost:8265
    >>> client = job_client()
    >>> # ... follow the same steps as above to submit a synchronous indexer job ...
    >>>
    >>> # OR use an explicit cluster launcher config file path
    >>> client = job_client("/Users/pdames/workspace/deltacat.yaml")
    >>> # ... follow the same steps as above to submit a synchronous indexer job ...
    """
    script_args = [
        (
            [
                "--source",
            ],
            {
                "help": "Source DeltaCAT URL to index.",
                "type": str,
            },
        ),
        (
            [
                "--dest",
            ],
            {
                "help": "Destination DeltaCAT URL to index.",
                "type": str,
            },
        ),
    ]
    # parse CLI input arguments
    parser = argparse.ArgumentParser()
    for args, kwargs in script_args:
        parser.add_argument(*args, **kwargs)
    args = parser.parse_args()
    print(f"Command Line Arguments: {args}")

    # initialize deltacat
    deltacat.init()

    # run the example using the parsed arguments
    run(**vars(args))

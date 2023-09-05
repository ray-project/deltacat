import ray

from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.types.media import StorageType
from deltacat.utils.performance import timed_invocation

ray.init(address="auto")


@ray.remote
def convert_sort_and_dedupe(pyarrow_table):
    pandas_dataframe = pyarrow_table.to_pandas()
    pandas_dataframe.sort_values(["sort_key_1"])
    pandas_dataframe.drop_duplicates(["dedupe_key_1", "dedupe_key_2"])
    return pandas_dataframe


def run_all(dc_storage_ray=unimplemented_deltacat_storage):
    """Run all examples."""
    deltas_list_result = ray.get(
        dc_storage_ray.list_deltas.remote(
            "TestProvider",
            "TestTable",
            ["1", "2018-03-06T00:00:00.000Z"],
        )
    )

    delta = deltas_list_result.read_page()[0]

    pa_table_pending_ids = ray.get(
        dc_storage_ray.download_delta.remote(delta),
        storage_type=StorageType.LOCAL,
    )

    pending_futures = []
    for table_pending_id in pa_table_pending_ids:
        pending_future = convert_sort_and_dedupe.remote(table_pending_id)
        pending_futures.append(pending_future)
    pandas_dataframes, latency = timed_invocation(ray.get, pending_futures)
    print(f"Time to read, convert, sort, and dedupe delta: {latency}s")


if __name__ == "__main__":
    run_all()

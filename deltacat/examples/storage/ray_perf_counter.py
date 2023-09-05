import ray

from deltacat import ListResult
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.utils.ray_utils.collections import DistributedCounter
from deltacat.utils.ray_utils.performance import invoke_with_perf_counter

ray.init(address="auto")


def list_all_tables_for_namespaces(
    namespaces, dc_storage=unimplemented_deltacat_storage
):

    namespace_tables_promises = {}
    for namespace in namespaces:
        namespace = namespace["namespace"]
        tables_list_result_promise = ListResult.all_items_ray.remote(
            dc_storage.list_tables(namespace)
        )
        namespace_tables_promises[namespace] = tables_list_result_promise

    namespace_table_counts = {}
    tables = []
    for namespace, promise in namespace_tables_promises.items():
        namespace_tables = ray.get(promise)
        namespace_table_count = len(namespace_tables)
        namespace_table_counts[namespace] = namespace_table_count
        tables.extend(namespace_tables)
    sorted_namespace_table_counts = dict(
        sorted(namespace_table_counts.items(), key=lambda item: item[1])
    )
    print(f"Table counts by namespace: {sorted_namespace_table_counts}")
    print(f"Total tables: {len(tables)}")

    return tables


def run_all(dc_storage=unimplemented_deltacat_storage):
    """Run all examples."""

    distributed_counter = DistributedCounter.remote()
    namespaces, latency = invoke_with_perf_counter(
        distributed_counter,
        "list_all_namespaces",
        dc_storage.list_namespaces().all_items,
    )
    print(f"Total namespaces: {len(namespaces)}")
    print(f"List namespace latency: {latency}")

    tables, latency = invoke_with_perf_counter(
        distributed_counter,
        "list_all_tables",
        list_all_tables_for_namespaces,
        namespaces,
        dc_storage,
    )
    print(f"List tables latency: {latency}")


if __name__ == "__main__":
    run_all()

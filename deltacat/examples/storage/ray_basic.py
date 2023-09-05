import ray

from deltacat.storage import interface as unimplemented_deltacat_storage

ray.init(address="auto")


def run_all(dc_storage_ray=unimplemented_deltacat_storage):
    """Run all examples."""

    # make an asynchronous call to list namespaces
    list_namespaces_future = dc_storage_ray.list_namespaces.remote()

    # gather the first page of namespaces synchronously
    namespaces_page_one = ray.get(list_namespaces_future)
    print(f"First page of Namespaces: {namespaces_page_one}")

    # make asynchronous invocations to list tables for the first 10 namespaces
    pending_futures = []
    for i in range(10):
        namespace = namespaces_page_one.read_page()[i]["namespace"]
        list_tables_future = dc_storage_ray.list_tables.remote(namespace)
        pending_futures.append(list_tables_future)

    # asynchronously gather each table listing in the order they complete
    while len(pending_futures):
        ready_futures, pending_futures = ray.wait(pending_futures)
        tables = ray.get(ready_futures[0])
        print(f"Received one page of tables: {tables}")


if __name__ == "__main__":
    run_all()

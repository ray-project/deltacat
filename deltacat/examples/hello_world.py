import ray
import deltacat as dc
import daft


def print_package_version_info():
    print(f"DeltaCAT Version: {dc.__version__}")
    print(f"Ray Version: {ray.__version__}")
    print(f"Daft Version: {daft.__version__}")


@ray.remote
def hello_worker():
    print("Hello, Worker!")
    df = daft.from_pydict({"hello": ["delta", "cat"]})
    dc.write(df, "hello_world")
    print_package_version_info()


def run():
    print("Hello, Driver!")
    print_package_version_info()
    ray.get(hello_worker.remote())
    df = dc.read("hello_world")
    print("=== Table Written by Ray Worker ===")
    print(df)


if __name__ == "__main__":
    # initialize deltacat
    # Catalog files will be stored in .deltacat/ in the current working directory.
    dc.init_local()

    # run the example
    run()

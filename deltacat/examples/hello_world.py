import ray
import deltacat
import daft
import pyiceberg

from deltacat.examples.common.fixtures import store_cli_args_in_os_environ


def print_package_version_info():
    print(f"DeltaCAT Version: {deltacat.__version__}")
    print(f"PyIceberg Version: {pyiceberg.__version__}")
    print(f"Ray Version: {ray.__version__}")
    print(f"Daft Version: {daft.__version__}")


@ray.remote
def hello_worker():
    print("Hello, Worker!")
    print_package_version_info()


def run():
    print("Hello, Driver!")
    print_package_version_info()
    hello_worker.remote()


if __name__ == "__main__":
    run()

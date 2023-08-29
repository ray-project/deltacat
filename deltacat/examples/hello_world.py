import ray
import deltacat


@ray.remote
def hello_worker():
    print("Hello, Worker!")
    print(f"Worker DeltaCAT Version: {deltacat.__version__}")


def run():
    print("Hello, Driver!")
    print(f"Driver DeltaCAT Version: {deltacat.__version__}")
    hello_worker.remote()


if __name__ == "__main__":
    run()

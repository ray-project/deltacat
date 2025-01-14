import sys
import time
from contextlib import contextmanager
from typing import Generator, Tuple, Iterator

from deltacat.storage.rivulet.dataset import Dataset
from deltacat.storage.rivulet.reader.query_expression import QueryExpression
from .benchmark_report import BenchmarkStep, BenchmarkMetric


@contextmanager
def timed_step(description: str) -> Generator[BenchmarkStep, None, None]:
    """Convenience for computing elapsed time of a block of code as a metric.

    :param description: description of the step
    :return: a benchmark operation populated with the elapsed time
    """
    metric = BenchmarkStep(description)
    start_time = time.time()
    yield metric
    end_time = time.time()
    metric.add(BenchmarkMetric("elapsed_time", 1000 * (end_time - start_time), "ms"))


class BenchmarkEngine:

    def __init__(self, dataset: Dataset):
        self.dataset = dataset

    def load_and_commit(self, field_group, generator, count) -> Tuple[str, BenchmarkStep]:
        """Load count number of rows from the generator and commit.

        :param generator: row generator
        :param count: the number of rows to load into the dataset
        :return: tuple of the manifest URI and a operation measurement
        """
        desc = f"load {count} from {generator}"
        writer = self.dataset.writer(field_group)
        with timed_step(desc) as step:
            rows = [generator.generate() for _ in range(count)]
            writer.write(rows)
            result = writer.flush()
        step.add(BenchmarkMetric("loaded", count))
        return result, step

    def scan(self) -> Tuple[set[any], BenchmarkStep]:
        """Scans the rows and prints some basic statistics about the manifest"""
        keys = set()
        object_count = 0
        size_b = 0
        with timed_step("full scan") as step:
            for row in self.dataset.scan(QueryExpression()).to_pydict():
                object_count += 1
                size_b += sum([sys.getsizeof(x) for x in row.values()])
                keys.add(row.get(self.dataset.schema.primary_key.name))
                # TODO replace with the actual metrics we want to measure
        step.add(BenchmarkMetric("rows read", object_count))
        step.add(BenchmarkMetric("size", size_b / (1024 * 1024), "MB"))
        return keys, step

    def run_queries(self, description, manifest_uri, queries: list[QueryExpression]) -> BenchmarkStep:
        object_count = 0
        size_b = 0
        with timed_step(description) as step:
            for query in queries:
                for row in self.dataset.scan(query).to_pydict():
                    object_count += 1
                    size_b += sum([sys.getsizeof(x) for x in row.values()])
        # TODO replace with the actual metrics we want to measure
        step.add(BenchmarkMetric("rows read", object_count))
        step.add(BenchmarkMetric("size", size_b / (1024 * 1024), "MB"))
        return step

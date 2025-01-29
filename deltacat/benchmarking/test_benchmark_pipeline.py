import math
from random import shuffle
import pytest
from deltacat.storage.rivulet.dataset import Dataset
from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet.reader.query_expression import QueryExpression
from deltacat.storage.rivulet.schema.schema import Schema
from deltacat.benchmarking.benchmark_engine import BenchmarkEngine
from deltacat.benchmarking.benchmark_report import BenchmarkRun, BenchmarkReport
from deltacat.benchmarking.benchmark_suite import BenchmarkSuite
from deltacat.benchmarking.data.random_row_generator import RandomRowGenerator
from deltacat.benchmarking.data.row_generator import RowGenerator
from deltacat.tests.test_utils.filesystem import temp_dir_autocleanup

pytestmark = pytest.mark.benchmark


@pytest.fixture
def schema():
    return Schema(
        [
            ("id", Datatype.int32()),
            ("source", Datatype.string()),
            ("media", Datatype.image("png")),
        ],
        "id",
    )


class LoadAndScanSuite(BenchmarkSuite):
    """Load some number of rows and scan"""

    schema_name = "LoadAndScanSuite"

    def __init__(self, dataset: Dataset, schema: Schema, generator, description=None):
        self.suite = "ReadSuite"
        self.dataset: Dataset = dataset
        self.schema = schema
        self.dataset.add_schema(schema, LoadAndScanSuite.schema_name)
        self.generator: RowGenerator = generator
        self.description: str = description or f"{self.dataset} x {self.generator}"

    def run(self) -> BenchmarkRun:
        container = BenchmarkEngine(self.dataset)
        run = BenchmarkRun(self.suite, self.description)
        # load a large number of rows
        manifest_uri, step = container.load_and_commit(
            LoadAndScanSuite.schema_name, self.generator, 1000
        )
        run.add(step)
        # do a full scan of all rows (and eagerly load them)
        keys, step = container.scan()
        run.add(step)
        # randomly retrieve all keys one-by-one from the dataset
        random_keys = list(keys)
        shuffle(random_keys)
        step = container.run_queries(
            "load all keys individually",
            manifest_uri,
            [QueryExpression().with_key(k) for k in random_keys],
        )
        run.add(step)
        # split into 4 key ranges and get them individually
        quartiles = self._generate_quartiles(keys)
        expressions = [
            QueryExpression().with_range(start, end) for (start, end) in quartiles
        ]
        step = container.run_queries(
            "load key ranges by quartile", manifest_uri, expressions
        )
        run.add(step)
        return run

    @staticmethod
    def _generate_quartiles(keys):
        sorted_keys = sorted(keys)
        size = len(keys)
        starts = list(range(0, size, math.ceil(size / 4)))
        ends = list([x - 1 for x in starts[1:]])
        ends.append(size - 1)
        quartiles = list(zip(starts, ends))
        return [(sorted_keys[start], sorted_keys[end]) for (start, end) in quartiles]


def test_suite1(schema: Schema, report: BenchmarkReport):
    with temp_dir_autocleanup() as temp_dir:
        generator = RandomRowGenerator(123, temp_dir)
        report.add(
            LoadAndScanSuite(
                Dataset(dataset_name="test_suite1_ds1", metadata_uri=temp_dir),
                schema,
                generator,
                "SST (rand)",
            ).run()
        )

    with temp_dir_autocleanup() as temp_dir:
        generator = RandomRowGenerator(123, temp_dir)
        report.add(
            LoadAndScanSuite(
                Dataset(dataset_name="test_suite1_ds2", metadata_uri=temp_dir),
                schema,
                generator,
                "dupe",
            ).run()
        )

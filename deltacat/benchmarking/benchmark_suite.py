from typing import Protocol

from deltacat.benchmarking.benchmark_report import BenchmarkRun


class BenchmarkSuite(Protocol):
    def run(self) -> BenchmarkRun:
        """Run the benchmark suite and produce a report.

        Each report should be comparable against other reports by the same suite"""
        ...

from dataclasses import dataclass
from tabulate import tabulate
from typing import Union, Optional


@dataclass
class BenchmarkMetric:
    name: str
    value: Union[float, int]
    unit: Optional[str] = None


class BenchmarkStep:
    """Captures measurements from a given operation"""

    def __init__(self, description):
        self.description: str = description
        """Description of the operation"""
        self._metrics: dict[str, BenchmarkMetric] = {}
        """Description of the operation"""

    def add(self, metric: BenchmarkMetric):
        self._metrics[metric.name] = metric

    def list_metrics(self):
        """List the metrics (sorted by name)"""
        return sorted(self._metrics.values(), key=lambda x: x.name)


class BenchmarkRun:
    """Class for capturing measurements for a given test suite for comparison."""

    def __init__(self, suite: str, description: str):
        self.suite = suite
        """The test suite associated with this report."""
        self.description = description
        """Description of the report"""
        self.steps: list[BenchmarkStep] = []
        """List of steps and their metrics"""

    def add(self, operation):
        self.steps.append(operation)


class BenchmarkReport:
    def __init__(self, name):
        self.name = name
        self.runs: list[BenchmarkRun] = []

    def add(self, run):
        self.runs.append(run)

    def __str__(self):
        """Pretty-print a table that compares the metrics across each report.

        We want to transpose these such that each report gets their own column and each metric gets its own row
        (ideally grouped by operation).
        """
        if not self.runs:
            print("No runs to compare!")
            return
        suites = set(r.suite for r in self.runs)
        if len(suites) > 1:
            print("Found more than one type of suite")
            return
        suite = self.runs[0].suite

        headers = [
            f"{suite} Operation",
            "Metric",
            "Unit",
            *[r.description for r in self.runs],
        ]
        rows = []
        for step_tranche in zip(*[r.steps for r in self.runs]):
            # TODO zip by metric name instead of assuming all metrics are being measured
            step_name = step_tranche[0].description
            for metric_tuple in zip(*[x.list_metrics() for x in step_tranche]):
                row = [
                    step_name,
                    metric_tuple[0].name,
                    metric_tuple[0].unit,
                    *[p.value for p in metric_tuple],
                ]
                rows.append(row)
        return tabulate(rows, headers=headers, tablefmt="fancy_outline")

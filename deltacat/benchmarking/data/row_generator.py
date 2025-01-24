from typing import Protocol, Iterator, Dict, Any


class RowGenerator(Protocol):
    def generate(self) -> Dict[str, Any]:
        ...

    def generate_dataset(self, count) -> Iterator[Dict[str, Any]]:
        """Generate a dataset with a given number of records"""
        return map(lambda x: self.generate(), iter(range(count)))

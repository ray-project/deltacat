import pyarrow.feather as feather
import pyarrow as pa


def metafile(cls):
    """
    Decorator to add file read and write methods to dict-based DeltaCAT model
    classes. Uses the Arrow IPC Feather V2 file format to serialize and
    deserialize metadata files.
    """

    def write(self, path: str):
        pa_table = pa.Table.from_pydict(self)
        feather.write_feather(pa_table, path)

    def read(path: str):
        pa_table = feather.read_table(path)
        return cls(**pa_table.to_pydict())

    cls.write = write
    cls.read = read

    return cls

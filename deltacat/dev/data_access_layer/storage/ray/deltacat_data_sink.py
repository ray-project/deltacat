import pyarrow as pa
from ray.data.block import BlockAccessor
from ray.data import Datasink
# Suppose these come from your code
# from deltacat.dev.data_access_layer.storage.writer import RivuletWriter, RivuletWriteOptions

class RivuletDatasink(Datasink):
    def __init__(
        self,
        dataset,
        file_format: str = "parquet",
        write_mode: str = "upsert",
        **kwargs
    ):
        """
        Initialize the Datasink with what you need to create an underlying Writer.
        """
        # This sink will create a RivuletWriter under the hood.
        self._dataset = dataset
        self._write_options = RivuletWriteOptions(
            write_mode=write_mode,
            file_format=file_format
        )

        # Underlying writer
        self._writer = RivuletWriter(dataset=self._dataset, file_format=file_format)

        # We will collect metadata from each block’s write call here on the worker.
        # Then in close(), we can finalize if needed.
        self._write_metadata = []

    def write(self, block: BlockAccessor) -> None:
        """
        Called in parallel on each worker for each block of data.
        """
        # Convert the Ray block to a pyarrow Table
        # (Ray might store data as Arrow, Pandas, or Python objects;
        #  so always do block.to_arrow() to get a PyArrow table.)
        arrow_table: pa.Table = block.to_arrow()

        # Optionally convert the table into a list of record batches
        batches = arrow_table.to_batches()

        # Now call your writer's write_batches
        # Because it's a generator, you can iterate to gather metadata
        # For Rivulet, the generator yields exactly one metadata per `write_batches` call,
        # but in principle you might yield multiple. We'll just gather them.
        gen = self._writer.write_batches(
            record_batches=batches,
            write_options=self._write_options
        )
        for m in gen:
            self._write_metadata.append(m)

    def close(self) -> None:
        """
        Called once on the cluster side (or driver side) after all blocks have been written.
        If your final commit must happen in a single place, you can do it here.
        """
        # In many Ray Data workflows, each worker calls close().
        # If you want a single aggregator commit, you will need
        # to rely on a shared location to store partial metadata or
        # do a final aggregator step after `.write_Datasink(...)`.
        #
        # But if your system does not strictly require collecting partial
        # metadata from all tasks in one place, you can finalize from here.
        # For Rivulet, it looks like you want a single commit.
        #
        # Naive approach: just commit with the metadata we have on *this* worker.
        # This might not be enough if there are multiple workers.
        # If your Rivulet dataset is truly local to each worker, or
        # if you are okay with multiple commits, this is fine.
        # Otherwise, you'll need a separate aggregator step.

        if self._write_metadata:
            self._writer.commit(write_metadata=self._write_metadata)
            self._write_metadata = []
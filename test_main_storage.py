import pytest
import shutil
import tempfile

from deltacat import Schema, Field
from deltacat.storage import metastore, Namespace, TableVersion, StreamFormat, CommitState, Table
from deltacat.storage.model.namespace import NamespaceLocator
from deltacat.catalog.main.impl import PropertyCatalog
import pyarrow as pa

# (other fixture definitions remain unchanged)

class TestStream:
    @classmethod
    def setup_class(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = PropertyCatalog(cls.tmpdir)
        # Create a table version for streams
        metastore.create_namespace("test_stream_ns", catalog=cls.catalog)
        metastore.create_table_version(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog=cls.catalog,
        )
        # Create the default stream for "deltacat" format
        cls.default_stream = metastore.create_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog=cls.catalog,
        )

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.tmpdir)

    def test_list_streams(self):
        list_result = metastore.list_streams(
            "test_stream_ns",
            "mystreamtable",
            "v1",
            catalog=self.catalog
        )
        streams = list_result.all_items()
        # We expect one stream (the default "deltacat" stream)
        assert len(streams) == 1

    def test_get_stream(self):
        stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog=self.catalog,
        )
        assert stream is not None
        # The stream's format should be the default "deltacat"
        assert stream.stream_format.lower() == StreamFormat.DELTACAT.value.lower()

    def test_create_stream_singleton_constraint(self):
        # Attempting to create another committed stream for the same table/version
        with pytest.raises(ValueError, match="A stream of format"):
            metastore.create_stream(
                namespace="test_stream_ns",
                table_name="mystreamtable",
                table_version="v1",
                catalog=self.catalog,
            )

    def test_stage_and_commit_stream_replacement(self):
        # Stage a replacement stream for the same table/version and stream format.
        staged_stream = metastore.stage_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            stream_format=StreamFormat.DELTACAT,
            catalog=self.catalog,
        )
        # The staged (STAGED) stream should record the previous stream's ID.
        assert staged_stream.previous_stream_id == self.default_stream.stream_id

        # Committing the staged stream should replace the previous stream.
        committed_stream = metastore.commit_stream(staged_stream, catalog=self.catalog)
        # Check that the new committed stream has a new unique id.
        assert committed_stream.stream_id != self.default_stream.stream_id

        # Now, calling create_stream again should fail as a stream exists.
        with pytest.raises(ValueError, match="A stream of format"):
            metastore.create_stream(
                namespace="test_stream_ns",
                table_name="mystreamtable",
                table_version="v1",
                catalog=self.catalog,
            ) 
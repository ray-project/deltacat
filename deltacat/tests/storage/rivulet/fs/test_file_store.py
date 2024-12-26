import os
import tempfile
from shutil import rmtree

from deltacat.storage.rivulet.fs.file_store import FileStore
from deltacat.storage.rivulet.fs.local_fs import LocalFS


class TestLocalFileStore:
    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.fs = LocalFS()
        self.file_store = FileStore()

    def teardown_method(self):
        rmtree(self.temp_dir)
        del self.fs
        del self.file_store

    def test_new_input_file(self):
        file_content = "foobar"
        os.makedirs(os.path.dirname(self.temp_dir), exist_ok=True)
        path = f"{self.temp_dir}/my-file"
        with open(path, "wt") as f:
            f.write(file_content)
        file = self.file_store.new_input_file(path)
        with file.open() as f:
            content = f.read()
            assert file_content.encode() == content

    def test_new_output_file(self):
        file_content = "foobar"
        path = f"{self.temp_dir}/my-file"
        file = self.file_store.new_output_file(path)
        with file.create() as f:
            f.write(file_content.encode())
        with open(path, "rt") as f:
            content = f.read()
            assert file_content == content

    def test_list_files(self):
        file_content = "foobar".encode()
        path1 = f"{self.temp_dir}/my-file"
        with self.file_store.new_output_file(path1).create() as f:
            f.write(file_content)
        path2 = f"{self.temp_dir}/other-file"
        with self.file_store.new_output_file(path2).create() as f:
            f.write(file_content)
        # should not include nested directories
        nested_dir = f"{self.temp_dir}/nested/"
        os.makedirs(os.path.dirname(nested_dir), exist_ok=True)
        path3 = f"{nested_dir}/nested-file"
        with self.file_store.new_output_file(path3).create() as f:
            f.write(file_content)
        file_names = set([f.location for f in self.fs.list_files(self.temp_dir)])
        assert file_names == {path1, path2}

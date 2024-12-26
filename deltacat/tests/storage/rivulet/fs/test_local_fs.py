import tempfile
from shutil import rmtree

import fsspec
import pytest

from deltacat.storage.rivulet.fs.file_system import (
    FSInputFile,
    FSOutputFile,
    FileSystem,
)
from deltacat.storage.rivulet.fs.fsspec_fs import FsspecFileSystem
from deltacat.storage.rivulet.fs.local_fs import LocalFS


@pytest.mark.parametrize("fs", [LocalFS(), FsspecFileSystem(fsspec.filesystem("file"))])
class TestLocalFileSystem:
    temp_dir: str
    """The (path) URI to pass into the FileSystem"""

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        rmtree(self.temp_dir)

    def test_file_system_creation_round_trip(self, fs: FileSystem):
        file_content = "foobar".encode()
        sub_dir = f"{self.temp_dir}/round-trip"
        path = f"{sub_dir}/my-file"

        assert not fs.exists(path)
        with fs.create(path) as f:
            f.write(file_content)
        # the file should exist
        assert fs.exists(path)
        # when we open the file, we should see the contents passed in during creation
        with fs.open(path) as f:
            content = f.read()
            assert file_content == content
        # listing the files should return only the created file
        file_names = [f.location for f in fs.list_files(sub_dir)]
        assert file_names == [path]

    def test_output_file_location(self, fs: FileSystem):
        uri = f"{self.temp_dir}/output-file"
        file = FSOutputFile(uri, fs)
        assert uri == file.location

    def test_output_file_exists(self, fs: FileSystem):
        uri = f"{self.temp_dir}/output-file"
        file = FSOutputFile(uri, fs)
        assert not file.exists()
        with fs.create(uri) as f:
            f.write("".encode())  # is there a touch equivalent?
        assert file.exists()

    def test_output_file_create(self, fs: FileSystem):
        uri = f"{self.temp_dir}/output-file"
        file_content = "foobar".encode()
        file = FSOutputFile(uri, fs)
        with file.create() as f:
            f.write(file_content)
        with fs.open(uri) as f:
            content = f.read()
            assert file_content == content

    def test_output_file_to_input_file(self, fs: FileSystem):
        uri = f"{self.temp_dir}/output-file"
        file_content = "foobar".encode()
        output_file = FSOutputFile(uri, fs)
        with output_file.create() as f:
            f.write(file_content)
        input_file = output_file.to_input_file()
        assert output_file.location == input_file.location
        with input_file.open() as f:
            assert f.read() == file_content

    def test_input_file_location(self, fs: FileSystem):
        uri = f"{self.temp_dir}/input-file"
        file = FSInputFile(uri, fs)
        assert uri == file.location

    def test_input_file_exists(self, fs: FileSystem):
        uri = f"{self.temp_dir}/input-file"
        file = FSInputFile(uri, fs)
        assert not file.exists()
        with fs.create(uri) as f:
            f.write("".encode())  # is there a touch equivalent?
        assert file.exists()

    def test_input_file_open(self, fs: FileSystem):
        uri = f"{self.temp_dir}/input-file"
        file_content = "foobar".encode()
        with fs.create(uri) as f:
            f.write(file_content)

        file = FSInputFile(uri, fs)
        with file.open() as f:
            assert f.read() == file_content

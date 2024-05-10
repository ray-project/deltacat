import unittest
from deltacat.aws.s3u import UuidBlockWritePathProvider, CapturedBlockWritePaths


class TestUuidBlockWritePathProvider(unittest.TestCase):
    def test_uuid_block_write_provider_sanity(self):
        capture_object = CapturedBlockWritePaths()
        provider = UuidBlockWritePathProvider(capture_object=capture_object)

        result = provider("base_path")

        self.assertRegex(result, r"^base_path/[\w-]{36}$")

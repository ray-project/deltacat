import pytest
from deltacat.storage.model.table_version import TableVersion

@pytest.mark.parametrize(
    "previous_version, expected_new_version",
    [
        (None, "metafile_"),  # Assuming Metafile.generate_new_id() returns something like this
        ("v1", "v2"),
        ("1", "2"),
        ("version1", "metafile_"),  # Assuming Metafile.generate_new_id() returns something like this
        ("v999", "v1000"),
    ]
)
def test_new_version(previous_version, expected_new_version):
    new_version = TableVersion.new_version(previous_version)
    assert isinstance(new_version, str)
    if previous_version is None or previous_version == "version1":
        assert new_version.startswith(expected_new_version)
    else:
        assert new_version == expected_new_version

# Add more tests as needed to cover edge cases and other scenarios 
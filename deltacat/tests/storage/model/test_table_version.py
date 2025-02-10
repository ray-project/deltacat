import pytest
from deltacat.storage.model.table_version import TableVersion

@pytest.mark.parametrize(
    "previous_version, expected_new_version",
    [
        (None, None),  # Assuming Metafile.generate_new_id() returns something like this
        ("v1", "v2"),
        ("1", "2"),
        ("version1", None),  # Assuming Metafile.generate_new_id() returns something like this
        ("v999", "v1000"),
    ]
)
def test_new_version(previous_version, expected_new_version):
    new_version = TableVersion.new_version(previous_version)
    assert isinstance(new_version, str)
    if expected_new_version:
        assert new_version==expected_new_version


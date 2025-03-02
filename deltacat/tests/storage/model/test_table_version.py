import pytest

from deltacat.constants import BYTES_PER_KIBIBYTE
from deltacat.storage.model.table_version import TableVersion, TableVersionLocator


@pytest.mark.parametrize(
    "previous_version, expected_next_version",
    [
        (None, "1"),
        ("v.1", "v.2"),
        ("1", "2"),
        ("1.0", "1.1"),
        ("v.999", "v.1000"),
    ],
)
def test_next_version(previous_version, expected_next_version):
    new_version = TableVersion.next_version(previous_version)
    assert isinstance(new_version, str)
    assert new_version == expected_next_version


@pytest.mark.parametrize(
    "table_version, expected_parsed_version",
    [
        ("v.1", ("v.", 1)),
        ("1", (None, 1)),
        ("1.0", ("1.", 0)),
        ("v.999", ("v.", 999)),
    ],
)
def test_parse_version(table_version, expected_parsed_version):
    prefix, version_number = TableVersion.parse_table_version(table_version)
    if prefix is not None:
        assert isinstance(prefix, str)
    assert isinstance(version_number, int)
    assert (prefix, version_number) == expected_parsed_version


def test_version_validation_invalid():
    with pytest.raises(ValueError):
        TableVersionLocator.at(
            namespace="test_namespace",
            table_name="test_table",
            table_version="invalid_version",
        )


def test_version_validation_valid_to_invalid():
    valid_tv_locator = TableVersionLocator.at(
        namespace="test_namespace",
        table_name="test_table",
        table_version="1",
    )
    assert valid_tv_locator.table_version == "1"
    tv = TableVersion.of(
        locator=valid_tv_locator,
        schema=None,
    )
    assert tv.current_version_number() == 1
    with pytest.raises(ValueError):
        valid_tv_locator.table_version = "invalid_version"


def test_version_validation_numeric_name():
    valid_tv_locator = TableVersionLocator.at(
        namespace="test_namespace",
        table_name="test_table",
        table_version="1.0",
    )
    assert valid_tv_locator.table_version == "1.0"
    tv = TableVersion.of(
        locator=valid_tv_locator,
        schema=None,
    )
    assert tv.current_version_number() == 0


def test_version_validation_truncate_leading_zeros():
    valid_tv_locator = TableVersionLocator.at(
        namespace="test_namespace",
        table_name="test_table",
        table_version="1.00002",
    )
    # ensure that leading 0's are truncated
    assert valid_tv_locator.table_version == "1.2"
    tv = TableVersion.of(
        locator=valid_tv_locator,
        schema=None,
    )
    assert tv.current_version_number() == 2


def test_version_validation_version_id_length_limits():
    # ensure that long version identifiers are accepted
    long_tv_id = "a" * (BYTES_PER_KIBIBYTE - 2) + ".1"
    valid_tv_locator = TableVersionLocator.at(
        namespace="test_namespace",
        table_name="test_table",
        table_version=long_tv_id,
    )
    assert valid_tv_locator.table_version == long_tv_id
    tv = TableVersion.of(
        locator=valid_tv_locator,
        schema=None,
    )
    assert tv.current_version_number() == 1
    # ensure that an excessively long version identifier is rejected
    with pytest.raises(ValueError):
        valid_tv_locator.table_version = "0" * (BYTES_PER_KIBIBYTE + 1)

import pytest

from deltacat.storage import (
    PartitionKey,
    PartitionScheme,
    IdentityTransform,
)


def test_partition_scheme_validates_empty_keys():
    # When creating a partition scheme with empty keys list
    with pytest.raises(
        ValueError, match="Partition scheme cannot have empty keys list"
    ):
        PartitionScheme.of(
            keys=[],
            name="test_partition_scheme",
            scheme_id="test_partition_scheme_id",
        )


def test_partition_scheme_validates_duplicate_keys():
    # When creating a partition scheme with duplicate keys
    with pytest.raises(ValueError, match="Duplicate partition key found: col1"):
        PartitionScheme.of(
            keys=[
                PartitionKey.of(
                    key=["col1"],
                    transform=IdentityTransform.of(),
                ),
                PartitionKey.of(
                    key=["col1"],  # Duplicate key
                    transform=IdentityTransform.of(),
                ),
            ],
            name="test_partition_scheme",
            scheme_id="test_partition_scheme_id",
        )


def test_partition_scheme_validates_duplicate_names():
    # When creating a partition scheme with duplicate partition key names
    with pytest.raises(
        ValueError, match="Duplicate partition key name found: partition_1"
    ):
        PartitionScheme.of(
            keys=[
                PartitionKey.of(
                    key=["col1"],
                    name="partition_1",
                    transform=IdentityTransform.of(),
                ),
                PartitionKey.of(
                    key=["col2"],  # Different field locator
                    name="partition_1",  # But duplicate name
                    transform=IdentityTransform.of(),
                ),
            ],
            name="test_partition_scheme",
            scheme_id="test_partition_scheme_id",
        )


def test_partition_scheme_allows_valid_keys():
    # When creating a partition scheme with valid keys
    partition_scheme = PartitionScheme.of(
        keys=[
            PartitionKey.of(
                key=["col1"],
                transform=IdentityTransform.of(),
            ),
            PartitionKey.of(
                key=["col2"],
                transform=IdentityTransform.of(),
            ),
        ],
        name="test_partition_scheme",
        scheme_id="test_partition_scheme_id",
    )

    # Then it should succeed
    assert partition_scheme is not None
    assert len(partition_scheme.keys) == 2
    assert partition_scheme.name == "test_partition_scheme"
    assert partition_scheme.id == "test_partition_scheme_id"

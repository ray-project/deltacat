"""Unit tests for the Delta model."""
import pytest

from deltacat.storage.model.delta import Delta, DeltaLocator


class TestDeltaLocator:
    """Tests for DeltaLocator stream_timestamp functionality."""

    def test_stream_timestamp_valid(self):
        """Test that valid millisecond timestamps are accepted."""
        locator = DeltaLocator.of(
            partition_locator=None,
            stream_position=123,
            stream_timestamp=1705849200000,  # Valid milliseconds timestamp
        )
        assert locator.stream_timestamp == 1705849200000
        assert locator.stream_position == 123

    def test_stream_timestamp_none(self):
        """Test that None timestamps are accepted."""
        locator = DeltaLocator.of(
            partition_locator=None,
            stream_position=123,
            stream_timestamp=None,
        )
        assert locator.stream_timestamp is None
        assert locator.stream_position == 123

    def test_stream_timestamp_invalid_too_small(self):
        """Test that timestamps that are too small are rejected."""
        locator = DeltaLocator.of()
        with pytest.raises(ValueError, match="stream_timestamp must be unix milliseconds"):
            locator.stream_timestamp = 123  # Invalid - not milliseconds (too small)

    def test_stream_timestamp_invalid_too_large(self):
        """Test that timestamps that are too large are rejected."""
        locator = DeltaLocator.of()
        with pytest.raises(ValueError, match="stream_timestamp must be unix milliseconds"):
            locator.stream_timestamp = 10_000_000_000_000  # Invalid - too large

    def test_stream_timestamp_boundary_min(self):
        """Test that the minimum valid timestamp is accepted."""
        locator = DeltaLocator.of()
        locator.stream_timestamp = 1_000_000_000_000  # Minimum valid
        assert locator.stream_timestamp == 1_000_000_000_000

    def test_stream_timestamp_boundary_max(self):
        """Test that the maximum valid timestamp is accepted."""
        locator = DeltaLocator.of()
        locator.stream_timestamp = 9_999_999_999_999  # Maximum valid
        assert locator.stream_timestamp == 9_999_999_999_999

    def test_stream_timestamp_persisted_as_camel_case(self):
        """Test that stream_timestamp is stored with camelCase key."""
        locator = DeltaLocator.of()
        locator.stream_timestamp = 1705849200000
        assert "streamTimestamp" in locator
        assert locator["streamTimestamp"] == 1705849200000


class TestDelta:
    """Tests for Delta stream_timestamp property."""

    def test_stream_timestamp_delegates_to_locator(self):
        """Test that Delta.stream_timestamp delegates to the locator."""
        locator = DeltaLocator.of(
            partition_locator=None,
            stream_position=123,
            stream_timestamp=1705849200000,
        )
        delta = Delta.of(
            locator=locator,
            delta_type=None,
            meta=None,
            properties=None,
            manifest=None,
        )
        assert delta.stream_timestamp == 1705849200000

    def test_stream_timestamp_returns_none_when_locator_is_none(self):
        """Test that Delta.stream_timestamp returns None when locator is None."""
        delta = Delta.of(
            locator=None,
            delta_type=None,
            meta=None,
            properties=None,
            manifest=None,
        )
        assert delta.stream_timestamp is None

    def test_stream_timestamp_returns_none_when_timestamp_not_set(self):
        """Test that Delta.stream_timestamp returns None when not set on locator."""
        locator = DeltaLocator.of(
            partition_locator=None,
            stream_position=123,
        )
        delta = Delta.of(
            locator=locator,
            delta_type=None,
            meta=None,
            properties=None,
            manifest=None,
        )
        assert delta.stream_timestamp is None

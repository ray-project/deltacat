import pytest
import pyarrow as pa
from datetime import date, datetime
from unittest.mock import patch

from deltacat.storage.model.expression import (
    Reference,
    Literal,
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanEqual,
    LessThanEqual,
    And,
    Or,
    Not,
    In,
    Between,
    Like,
    IsNull,
)

from deltacat.experimental.storage.iceberg.visitor import IcebergExpressionVisitor
from pyiceberg.expressions import (
    And as IceAnd,
    Or as IceOr,
    Not as IceNot,
    EqualTo,
    NotEqualTo,
    GreaterThan as IceGreaterThan,
    LessThan as IceLessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    IsNull as IceIsNull,
    In as IceIn,
)


@pytest.fixture
def iceberg_visitor():
    return IcebergExpressionVisitor()


class TestIcebergExpressionVisitor:
    """
    Tests for the PyIceberg expression visitor.
    """

    def test_references(self, iceberg_visitor):
        """Tests that references are formatted correctly."""
        assert iceberg_visitor.visit(Reference("partition_key")) == "partition_key"
        assert iceberg_visitor.visit(Reference("year")) == "year"
        assert (
            iceberg_visitor.visit(Reference("complex_name_with_underscores"))
            == "complex_name_with_underscores"
        )

    def test_string_literals(self, iceberg_visitor):
        """Tests that string literals are properly quoted."""
        assert iceberg_visitor.visit(Literal(pa.scalar("value"))) == "value"
        assert (
            iceberg_visitor.visit(Literal(pa.scalar("contains'quote")))
            == "contains'quote"
        )
        assert iceberg_visitor.visit(Literal(pa.scalar(""))) == ""

    def test_numeric_literals(self, iceberg_visitor):
        """Tests that numeric literals are formatted correctly."""
        assert iceberg_visitor.visit(Literal(pa.scalar(42))) == 42
        assert iceberg_visitor.visit(Literal(pa.scalar(3.14))) == 3.14
        assert iceberg_visitor.visit(Literal(pa.scalar(0))) == 0
        assert iceberg_visitor.visit(Literal(pa.scalar(-10))) == -10

    def test_boolean_literals(self, iceberg_visitor):
        """Tests that boolean literals are formatted correctly."""
        assert iceberg_visitor.visit(Literal(pa.scalar(True))) is True
        assert iceberg_visitor.visit(Literal(pa.scalar(False))) is False

    def test_date_timestamp_literals(self, iceberg_visitor):
        """Tests that date and timestamp literals are formatted correctly."""
        test_date = date(2023, 1, 15)
        test_datetime = datetime(2023, 1, 15, 14, 30, 45)
        assert iceberg_visitor.visit(Literal(pa.scalar(test_date))) == test_date
        assert iceberg_visitor.visit(Literal(pa.scalar(test_datetime))) == test_datetime

    def test_null_literal(self, iceberg_visitor):
        """Tests that NULL values are formatted correctly."""
        assert iceberg_visitor.visit(Literal(pa.scalar(None))) is None

    def test_comparison_operators(self, iceberg_visitor):
        """Tests comparison operators formatting."""
        year_ref = Reference("year")
        year_val = Literal(pa.scalar(2023))

        assert isinstance(iceberg_visitor.visit(Equal(year_ref, year_val)), EqualTo)
        assert isinstance(
            iceberg_visitor.visit(NotEqual(year_ref, year_val)), NotEqualTo
        )
        assert isinstance(
            iceberg_visitor.visit(GreaterThan(year_ref, year_val)), IceGreaterThan
        )
        assert isinstance(
            iceberg_visitor.visit(LessThan(year_ref, year_val)), IceLessThan
        )
        assert isinstance(
            iceberg_visitor.visit(GreaterThanEqual(year_ref, year_val)),
            GreaterThanOrEqual,
        )
        assert isinstance(
            iceberg_visitor.visit(LessThanEqual(year_ref, year_val)), LessThanOrEqual
        )

    def test_logical_operators(self, iceberg_visitor):
        """Tests logical operators formatting."""
        year_expr = Equal(Reference("year"), Literal(pa.scalar(2023)))
        month_expr = Equal(Reference("month"), Literal(pa.scalar(1)))

        assert isinstance(iceberg_visitor.visit(And(year_expr, month_expr)), IceAnd)
        assert isinstance(iceberg_visitor.visit(Or(year_expr, month_expr)), IceOr)
        assert isinstance(iceberg_visitor.visit(Not(year_expr)), IceNot)

    def test_is_null(self, iceberg_visitor):
        """Tests IS NULL formatting."""
        assert isinstance(
            iceberg_visitor.visit(IsNull(Reference("partition_key"))), IceIsNull
        )

    def test_in_operator(self, iceberg_visitor):
        """Tests IN operator formatting."""
        region_values = [
            Literal(pa.scalar("us-east-1")),
            Literal(pa.scalar("us-west-2")),
            Literal(pa.scalar("eu-west-1")),
        ]
        in_expr = In(Reference("region"), region_values)
        assert isinstance(iceberg_visitor.visit(in_expr), IceIn)

    def test_between_operator(self, iceberg_visitor):
        """Tests BETWEEN operator formatting."""
        between_expr = Between(
            Reference("month"), Literal(pa.scalar(1)), Literal(pa.scalar(3))
        )
        result = iceberg_visitor.visit(between_expr)
        assert isinstance(result, IceAnd)
        assert isinstance(result.left, GreaterThanOrEqual)
        assert isinstance(result.right, LessThanOrEqual)

    def test_like_operator(self, iceberg_visitor):
        """Tests LIKE operator formatting, LIKE is not supported"""
        like_expr = Like(Reference("filename"), Literal(pa.scalar("%data%.parquet")))

        with patch(
            "deltacat.experimental.storage.iceberg.visitor.logger"
        ) as mock_logger:
            result = iceberg_visitor.visit(like_expr)
            assert result is None
            mock_logger.warning.assert_called_once()
            warning_message = mock_logger.warning.call_args[0][0]
            assert "LIKE operation is not supported in PyIceberg" in warning_message

    def test_complex_expression(self, iceberg_visitor):
        """Tests a complex, nested expression."""
        year_expr = Equal(Reference("year"), Literal(pa.scalar(2023)))
        month_expr = Between(
            Reference("month"), Literal(pa.scalar(1)), Literal(pa.scalar(3))
        )
        region_values = [
            Literal(pa.scalar("us-east-1")),
            Literal(pa.scalar("us-west-2")),
        ]
        region_expr = In(Reference("region"), region_values)
        complex_expr = Or(And(year_expr, month_expr), region_expr)

        result = iceberg_visitor.visit(complex_expr)
        assert isinstance(result, IceOr)
        assert isinstance(result.left, IceAnd)
        assert isinstance(result.right, IceIn)

import logging
from typing import Any

import pyarrow
from deltacat.storage.model.scan.push_down import PartitionFilter

import deltacat.logs as logs
from deltacat.storage.model.expression import Reference, Literal
from deltacat.storage.model.expression.visitor import ExpressionVisitor
from pyiceberg.expressions import (
    And,
    Or,
    Not,
    EqualTo,
    NotEqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    IsNull,
    In,
)

# Initialize DeltaCAT logger
logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class IcebergExpressionVisitor(ExpressionVisitor[None, Any]):
    """
    Visitor that translates DeltaCAT expressions to PyIceberg expressions.
    """

    def visit(self, expr, context=None):
        # Handle PartitionFilter by extracting and visiting the inner expression
        if isinstance(expr, PartitionFilter):
            return self.visit(expr.expr, context)
        # Handle all other expressions using the parent's visit method
        return super().visit(expr, context)

    def visit_reference(self, expr: Reference, context=None) -> str:
        return expr.field

    def visit_literal(self, expr: Literal, context=None) -> Any:
        # Convert PyArrow scalar to Python native type
        return (
            expr.value.as_py() if isinstance(expr.value, pyarrow.Scalar) else expr.value
        )

    def visit_and(self, expr, context=None):
        left = self.visit(expr.left, context)
        right = self.visit(expr.right, context)
        return And(left, right)

    def visit_or(self, expr, context=None):
        left = self.visit(expr.left, context)
        right = self.visit(expr.right, context)
        return Or(left, right)

    def visit_not(self, expr, context=None):
        operand = self.visit(expr.operand, context)
        return Not(operand)

    def visit_equal(self, expr, context=None):
        left = self.visit(expr.left, context)
        right = self.visit(expr.right, context)
        return EqualTo(left, right)

    def visit_not_equal(self, expr, context=None):
        left = self.visit(expr.left, context)
        right = self.visit(expr.right, context)
        return NotEqualTo(left, right)

    def visit_greater_than(self, expr, context=None):
        left = self.visit(expr.left, context)
        right = self.visit(expr.right, context)
        return GreaterThan(left, right)

    def visit_greater_than_equal(self, expr, context=None):
        left = self.visit(expr.left, context)
        right = self.visit(expr.right, context)
        return GreaterThanOrEqual(left, right)

    def visit_less_than(self, expr, context=None):
        left = self.visit(expr.left, context)
        right = self.visit(expr.right, context)
        return LessThan(left, right)

    def visit_less_than_equal(self, expr, context=None):
        left = self.visit(expr.left, context)
        right = self.visit(expr.right, context)
        return LessThanOrEqual(left, right)

    def visit_is_null(self, expr, context=None):
        operand = self.visit(expr.operand, context)
        return IsNull(operand)

    def visit_in(self, expr, context=None):
        value = self.visit(expr.value, context)
        values = [self.visit(v, context) for v in expr.values]
        return In(value, values)

    def visit_between(self, expr, context=None):
        value = self.visit(expr.value, context)
        lower = self.visit(expr.lower, context)
        upper = self.visit(expr.upper, context)
        return And(GreaterThanOrEqual(value, lower), LessThanOrEqual(value, upper))

    # PyIceberg does not have a direct equivalent of LIKE
    def visit_like(self, expr, context=None):
        value = self.visit(expr.value, context)
        pattern = self.visit(expr.pattern, context)
        logger.warning(
            f"LIKE operation is not supported in PyIceberg. Ignoring LIKE filter: {value} LIKE '{pattern}'. "
            "This may result in more data being returned than expected."
        )
        # Return None or a default expression that won't filter anything
        return (
            None  # or return NotEqualTo(value, None)  # matches everything except NULL
        )

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from typing import List, TypeVar, Any

import pyarrow as pa

from deltacat.storage.model.schema import FieldName


R = TypeVar("R")
C = TypeVar("C")


@dataclass(frozen=True)
class Expression(ABC):
    """
    This abstract class serves as the foundation for all expression types
    in the Deltacat expression tree. It defines common functionality and structure
    for expressions.
    """

    def __str__(self) -> str:
        """Convert the expression to a string representation.

        Returns:
            str: A string representation of the expression.
        """
        from deltacat.storage.model.expression.visitor import DisplayVisitor

        return DisplayVisitor().visit(self, None)


@dataclass(frozen=True)
class UnaryExpression(Expression, ABC):
    """
    This abstract class represents operations that work on a single input
    expression (e.g., NOT, IS NULL).

    Args:
        operand: The expression this unary operation applies to.
    """

    operand: Expression

    @classmethod
    def of(cls, operand: Expression | Any) -> UnaryExpression:
        """
        Create a new instance with the given operand.

        Args:
            operand: The expression or value to use as the operand.
                If not an Expression, it will be converted to a Literal.

        Returns:
            UnaryExpression: A new instance of this unary expression.
        """
        operand_expr = (
            operand if isinstance(operand, Expression) else Literal(pa.scalar(operand))
        )
        return cls(operand_expr)


@dataclass(frozen=True)
class BinaryExpression(Expression, ABC):
    """
    This abstract class represents operations that work on two input
    expressions (e.g., AND, OR, comparison operators).

    Args:
        left: The left operand expression.
        right: The right operand expression.
    """

    left: Expression
    right: Expression

    def with_left(self, left: Expression) -> BinaryExpression:
        """
        Create a new expression with a different left operand.

        Args:
            left: The new left operand expression.

        Returns:
            BinaryExpression: A new binary expression with the updated left operand.
        """
        return type(self)(left, self.right)

    def with_right(self, right: Expression) -> BinaryExpression:
        """
        Create a new expression with a different right operand.

        Args:
            right: The new right operand expression.

        Returns:
            BinaryExpression: A new binary expression with the updated right operand.
        """
        return type(self)(self.left, right)

    @classmethod
    def of(cls, left: Expression | Any, right: Expression | Any) -> BinaryExpression:
        """
        Create a new instance with the given operands.

        Args:
            left: The left operand expression or value.
                If not an Expression, it will be converted to a Literal.
            right: The right operand expression or value.
                If not an Expression, it will be converted to a Literal.

        Returns:
            BinaryExpression: A new instance of this binary expression.
        """
        left_expr = left if isinstance(left, Expression) else Literal(pa.scalar(left))
        right_expr = (
            right if isinstance(right, Expression) else Literal(pa.scalar(right))
        )
        return cls(left_expr, right_expr)


@dataclass(frozen=True)
class BooleanExpression(Expression, ABC):
    """
    This abstract class represents expressions that produce a boolean
    value when evaluated, such as comparisons, logical operations, etc.
    """

    def and_(self, other: BooleanExpression | Any) -> BooleanExpression:
        """
        Combine this expression with another using logical AND.

        Args:
            other: The expression to AND with this one.
                If not a BooleanExpression, it will be converted to a Literal.

        Returns:
            BooleanExpression: A new AND expression.
        """
        other_expr = (
            other if isinstance(other, BooleanExpression) else Literal(pa.scalar(other))
        )
        return And(self, other_expr)

    def or_(self, other: BooleanExpression | Any) -> BooleanExpression:
        """
        Combine this expression with another using logical OR.

        Args:
            other: The expression to OR with this one.
                If not a BooleanExpression, it will be converted to a Literal.

        Returns:
            BooleanExpression: A new OR expression.
        """
        other_expr = (
            other if isinstance(other, BooleanExpression) else Literal(pa.scalar(other))
        )
        return Or(self, other_expr)

    def not_(self) -> BooleanExpression:
        """
        Negate this expression with logical NOT.

        Returns:
            BooleanExpression: A new NOT expression.
        """
        return Not(self)


@dataclass(frozen=True)
class Reference(Expression):
    """
    This class represents a reference to a column or field in a dataset.

    TODO: Add validation and deltacat schema binding.

    Args:
        field: The field name of the referenced field.
        index: Optional index value for the field in the schema.
    """

    field: FieldName
    index: int | None = None

    @classmethod
    def of(cls, field: FieldName) -> Reference:
        """
        Create a new reference.

        Args:
            field: The field name of a Deltacat field.

        Returns:
            Reference: A new reference expression.
        """
        return cls(field)

    def eq(self, other: Expression | Any) -> Equal:
        """
        Create an equality comparison with this reference.

        Args:
            other: The expression or value to compare with.
                If not an Expression, it will be converted to a Literal.

        Returns:
            Equal: A new equality comparison expression.
        """
        if not isinstance(other, Expression):
            other = Literal(pa.scalar(other))
        return Equal(self, other)

    def ne(self, other: Expression | Any) -> NotEqual:
        """
        Create an inequality comparison with this reference.

        Args:
            other: The expression or value to compare with.
                If not an Expression, it will be converted to a Literal.

        Returns:
            NotEqual: A new inequality comparison expression.
        """
        if not isinstance(other, Expression):
            other = Literal(pa.scalar(other))
        return NotEqual(self, other)

    def gt(self, other: Expression | Any) -> GreaterThan:
        """
        Create a greater than comparison with this reference.

        Args:
            other: The expression or value to compare with.
                If not an Expression, it will be converted to a Literal.

        Returns:
            GreaterThan: A new greater than comparison expression.
        """
        if not isinstance(other, Expression):
            other = Literal(pa.scalar(other))
        return GreaterThan(self, other)

    def lt(self, other: Expression | Any) -> LessThan:
        """
        Create a less than comparison with this reference.

        Args:
            other: The expression or value to compare with.
                If not an Expression, it will be converted to a Literal.

        Returns:
            LessThan: A new less than comparison expression.
        """
        if not isinstance(other, Expression):
            other = Literal(pa.scalar(other))
        return LessThan(self, other)

    def ge(self, other: Expression | Any) -> GreaterThanEqual:
        """
        Create a greater than or equal comparison with this reference.

        Args:
            other: The expression or value to compare with.
                If not an Expression, it will be converted to a Literal.

        Returns:
            GreaterThanEqual: A new greater than or equal comparison expression.
        """
        if not isinstance(other, Expression):
            other = Literal(pa.scalar(other))
        return GreaterThanEqual(self, other)

    def le(self, other: Expression | Any) -> LessThanEqual:
        """
        Create a less than or equal comparison with this reference.

        Args:
            other: The expression or value to compare with.
                If not an Expression, it will be converted to a Literal.

        Returns:
            LessThanEqual: A new less than or equal comparison expression.
        """
        if not isinstance(other, Expression):
            other = Literal(pa.scalar(other))
        return LessThanEqual(self, other)

    def is_null(self) -> IsNull:
        """
        Create an IS NULL check for this reference.

        Returns:
            IsNull: A new IS NULL expression.
        """
        return IsNull(self)

    def in_(self, values: List[Expression | Any]) -> In:
        """
        Create an IN check for this reference.

        Args:
            values: List of expressions or values to check against.
                Non-Expression values will be converted to Literals.

        Returns:
            In: A new IN expression.
        """
        expr_values = [
            v if isinstance(v, Expression) else Literal(pa.scalar(v)) for v in values
        ]
        return In(self, expr_values)

    def between(self, lower: Expression | Any, upper: Expression | Any) -> Between:
        """
        Create a BETWEEN check for this reference.

        Args:
            lower: The lower bound expression or value.
                If not an Expression, it will be converted to a Literal.
            upper: The upper bound expression or value.
                If not an Expression, it will be converted to a Literal.

        Returns:
            Between: A new BETWEEN expression.
        """
        lower_expr = (
            lower if isinstance(lower, Expression) else Literal(pa.scalar(lower))
        )
        upper_expr = (
            upper if isinstance(upper, Expression) else Literal(pa.scalar(upper))
        )
        return Between(self, lower_expr, upper_expr)

    def like(self, pattern: str | Expression) -> Like:
        """
        Create a LIKE pattern match for this reference.

        Args:
            pattern: The pattern to match against.
                If not an Expression, it will be converted to a Literal.

        Returns:
            Like: A new LIKE expression.
        """
        pattern_expr = (
            pattern if isinstance(pattern, Expression) else Literal(pa.scalar(pattern))
        )
        return Like(self, pattern_expr)


@dataclass(frozen=True)
class Literal(Expression):
    """
    Literal value using PyArrow scalar.

    TODO: implement custom deltacat types and literals.

    Args:
        value: The PyArrow scalar value.
    """

    value: pa.Scalar

    @classmethod
    def of(cls, value: Any) -> Literal:
        """
        Create a new literal value.

        Args:
            value: The value to create a literal for.
                If not a PyArrow Scalar, it will be converted.

        Returns:
            Literal: A new literal expression.
        """
        if not isinstance(value, pa.Scalar):
            value = pa.scalar(value)
        return cls(value)

    def eq(self, other: Expression | Any) -> Equal:
        """
        Create an equality comparison with this literal.

        Args:
            other: The expression or value to compare with.
                If not an Expression, it will be converted to a Literal.

        Returns:
            Equal: A new equality comparison expression.
        """
        if not isinstance(other, Expression):
            other = Literal(pa.scalar(other))
        return Equal(self, other)

    def ne(self, other: Expression | Any) -> NotEqual:
        """
        Create an inequality comparison with this literal.

        Args:
            other: The expression or value to compare with.
                If not an Expression, it will be converted to a Literal.

        Returns:
            NotEqual: A new inequality comparison expression.
        """
        if not isinstance(other, Expression):
            other = Literal(pa.scalar(other))
        return NotEqual(self, other)


@dataclass(frozen=True)
class Equal(BinaryExpression, BooleanExpression):
    """
    Equality comparison (=).

    Args:
        left: The left operand expression.
        right: The right operand expression.
    """

    pass


@dataclass(frozen=True)
class NotEqual(BinaryExpression, BooleanExpression):
    """
    Inequality comparison (<>).

    Args:
        left: The left operand expression.
        right: The right operand expression.
    """

    pass


@dataclass(frozen=True)
class GreaterThan(BinaryExpression, BooleanExpression):
    """
    Greater than comparison (>).

    Args:
        left: The left operand expression.
        right: The right operand expression.
    """

    pass


@dataclass(frozen=True)
class LessThan(BinaryExpression, BooleanExpression):
    """
    Less than comparison (<).

    Args:
        left: The left operand expression.
        right: The right operand expression.
    """

    pass


@dataclass(frozen=True)
class GreaterThanEqual(BinaryExpression, BooleanExpression):
    """
    Greater than or equal comparison (>=).

    Args:
        left: The left operand expression.
        right: The right operand expression.
    """

    pass


@dataclass(frozen=True)
class LessThanEqual(BinaryExpression, BooleanExpression):
    """
    Less than or equal comparison (<=).

    Args:
        left: The left operand expression.
        right: The right operand expression.
    """

    pass


@dataclass(frozen=True)
class And(BinaryExpression, BooleanExpression):
    """
    Logical AND operation.

    Args:
        left: The left operand expression.
        right: The right operand expression.
    """

    pass


@dataclass(frozen=True)
class Or(BinaryExpression, BooleanExpression):
    """
    Logical OR operation.

    Args:
        left: The left operand expression.
        right: The right operand expression.
    """

    pass


@dataclass(frozen=True)
class Not(UnaryExpression, BooleanExpression):
    """
    Logical NOT operation.

    Args:
        operand: The boolean expression to negate.
    """

    operand: BooleanExpression


@dataclass(frozen=True)
class In(BooleanExpression):
    """
    Checks if a value is in a list of values.

    Args:
        value: The expression to check.
        values: List of expressions to check against.
    """

    value: Expression
    values: List[Expression] = field(default_factory=list)

    @classmethod
    def of(cls, value: Expression | Any, values: List[Expression | Any]) -> In:
        """
        Create a new IN expression.

        Args:
            value: The expression or value to check.
                If not an Expression, it will be converted to a Literal.
            values: List of expressions or values to check against.
                Non-Expression values will be converted to Literals.

        Returns:
            In: A new IN expression.
        """
        value_expr = (
            value if isinstance(value, Expression) else Literal(pa.scalar(value))
        )
        value_exprs = [
            v if isinstance(v, Expression) else Literal(pa.scalar(v)) for v in values
        ]
        return cls(value_expr, value_exprs)


@dataclass(frozen=True)
class Between(BooleanExpression):
    """
    Checks if a value is between two other values (inclusive).

    Args:
        value: The expression to check.
        lower: The lower bound expression.
        upper: The upper bound expression.
    """

    value: Expression
    lower: Expression
    upper: Expression

    @classmethod
    def of(
        cls, value: Expression | Any, lower: Expression | Any, upper: Expression | Any
    ) -> Between:
        """
        Create a new BETWEEN expression.

        Args:
            value: The expression or value to check.
                If not an Expression, it will be converted to a Literal.
            lower: The lower bound expression or value.
                If not an Expression, it will be converted to a Literal.
            upper: The upper bound expression or value.
                If not an Expression, it will be converted to a Literal.

        Returns:
            Between: A new BETWEEN expression.
        """
        value_expr = (
            value if isinstance(value, Expression) else Literal(pa.scalar(value))
        )
        lower_expr = (
            lower if isinstance(lower, Expression) else Literal(pa.scalar(lower))
        )
        upper_expr = (
            upper if isinstance(upper, Expression) else Literal(pa.scalar(upper))
        )
        return cls(value_expr, lower_expr, upper_expr)


@dataclass(frozen=True)
class Like(BooleanExpression):
    """
    Pattern matching for string values.

    Args:
        value: The expression to check.
        pattern: The pattern to match against.
    """

    value: Expression
    pattern: Expression

    @classmethod
    def of(cls, value: Expression | Any, pattern: Expression | Any) -> Like:
        """
        Create a new LIKE expression.

        Args:
            value: The expression or value to check.
                If not an Expression, it will be converted to a Literal.
            pattern: The pattern expression or value to match against.
                If not an Expression, it will be converted to a Literal.

        Returns:
            Like: A new LIKE expression.
        """
        value_expr = (
            value if isinstance(value, Expression) else Literal(pa.scalar(value))
        )
        pattern_expr = (
            pattern if isinstance(pattern, Expression) else Literal(pa.scalar(pattern))
        )
        return cls(value_expr, pattern_expr)


@dataclass(frozen=True)
class IsNull(UnaryExpression, BooleanExpression):
    """
    Checks if a value is NULL.

    Args:
        operand: The expression to check for NULL.
    """

    pass

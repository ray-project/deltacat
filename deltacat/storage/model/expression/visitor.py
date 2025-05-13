from abc import ABC, abstractmethod
from typing import Dict, Generic, TypeVar, Callable, Optional
from functools import singledispatchmethod
import re

from deltacat.storage.model.expression import (
    Expression,
    Reference,
    Literal,
    BinaryExpression,
    UnaryExpression,
    In,
    Between,
    Like,
)


C = TypeVar("C")  # Context type
R = TypeVar("R")  # Return type


class ExpressionVisitor(ABC, Generic[C, R]):
    """
    Visitor pattern for deltacat expressions.

    This base class provides two ways to implement visitors:
    1. Using a procedure dictionary (_PROCEDURES) - for simple, declarative visitors
    2. Using specialized visit_xyz methods with snake_case naming - for more control

    Subclasses need only implement visit_reference and visit_literal, plus either:
    - Define _PROCEDURES dictionary with functions for handling different expression types
    - Implement specific visit_xyz methods (using snake_case) for individual expressions
    """

    # Default procedure dictionary for subclasses to override
    _PROCEDURES: Dict[str, Callable] = {}

    def __init__(self):
        """Initialize visitor and validate required methods."""
        # Pre-check for required methods
        if not hasattr(self, "visit_reference") or not callable(
            getattr(self, "visit_reference")
        ):
            raise NotImplementedError("Subclasses must implement visit_reference")
        if not hasattr(self, "visit_literal") or not callable(
            getattr(self, "visit_literal")
        ):
            raise NotImplementedError("Subclasses must implement visit_literal")
        self._setup_default_procedure_handlers()

    def _to_snake_case(self, name: str) -> str:
        """Convert PascalCase or camelCase to snake_case."""
        pattern = re.compile(r"(?<!^)(?=[A-Z])")
        return pattern.sub("_", name).lower()

    def _setup_default_procedure_handlers(self):
        """Set up default procedure application methods if not overridden."""
        if not hasattr(self, "_apply_binary") or not callable(
            getattr(self, "_apply_binary")
        ):
            self._apply_binary = lambda proc, left, right: proc(left, right)
        if not hasattr(self, "_apply_unary") or not callable(
            getattr(self, "_apply_unary")
        ):
            self._apply_unary = lambda proc, operand: proc(operand)
        if not hasattr(self, "_apply_in") or not callable(getattr(self, "_apply_in")):
            self._apply_in = lambda proc, value, values: proc(value, values)
        if not hasattr(self, "_apply_between") or not callable(
            getattr(self, "_apply_between")
        ):
            self._apply_between = lambda proc, value, lower, upper: proc(
                value, lower, upper
            )
        if not hasattr(self, "_apply_like") or not callable(
            getattr(self, "_apply_like")
        ):
            self._apply_like = lambda proc, value, pattern: proc(value, pattern)

    @singledispatchmethod
    def visit(self, expr: Expression, context: Optional[C] = None) -> R:
        """
        Generic visit method that dispatches to specific methods based on expression type.

        Args:
            expr: The expression to visit
            context: Optional context to pass through the visitor

        Returns:
            Result of visiting the expression
        """
        expr_type = type(expr).__name__
        raise NotImplementedError(f"No visit method for type {expr_type}")

    @visit.register
    def _visit_reference(self, expr: Reference, context: Optional[C] = None) -> R:
        """Visit a Reference expression."""
        return self.visit_reference(expr, context)

    @visit.register
    def _visit_literal(self, expr: Literal, context: Optional[C] = None) -> R:
        """Visit a Literal expression."""
        return self.visit_literal(expr, context)

    @visit.register
    def _visit_binary(self, expr: BinaryExpression, context: Optional[C] = None) -> R:
        """Visit a binary expression using method specialization or procedures."""
        expr_type = type(expr).__name__

        left_result = self.visit(expr.left, context)
        right_result = self.visit(expr.right, context)

        method_name = f"visit_{self._to_snake_case(expr_type)}"
        if hasattr(self, method_name):
            method = getattr(self, method_name)
            return method(expr, context)

        if expr_type in self._PROCEDURES:
            return self._apply_binary(
                self._PROCEDURES[expr_type], left_result, right_result
            )

        try:
            return self.visit_binary_expression(
                expr, left_result, right_result, context
            )
        except NotImplementedError:
            raise NotImplementedError(f"No handler for {expr_type}")

    @visit.register
    def _visit_unary(self, expr: UnaryExpression, context: Optional[C] = None) -> R:
        """Visit a unary expression using method specialization or procedures."""
        expr_type = type(expr).__name__

        operand_result = self.visit(expr.operand, context)

        method_name = f"visit_{self._to_snake_case(expr_type)}"
        if hasattr(self, method_name):
            method = getattr(self, method_name)
            return method(expr, context)

        if expr_type in self._PROCEDURES:
            return self._apply_unary(self._PROCEDURES[expr_type], operand_result)

        try:
            return self.visit_unary_expression(expr, operand_result, context)
        except NotImplementedError:
            raise NotImplementedError(f"No handler for {expr_type}")

    @visit.register
    def _visit_in(self, expr: In, context: Optional[C] = None) -> R:
        """Visit an In expression."""
        if hasattr(self, "visit_in"):
            return self.visit_in(expr, context)

        if "In" in self._PROCEDURES:
            value_result = self.visit(expr.value, context)
            values_results = [self.visit(v, context) for v in expr.values]
            return self._apply_in(self._PROCEDURES["In"], value_result, values_results)

        raise NotImplementedError("No handler for In expression")

    @visit.register
    def _visit_between(self, expr: Between, context: Optional[C] = None) -> R:
        """Visit a Between expression."""
        if hasattr(self, "visit_between"):
            return self.visit_between(expr, context)

        if "Between" in self._PROCEDURES:
            value_result = self.visit(expr.value, context)
            lower_result = self.visit(expr.lower, context)
            upper_result = self.visit(expr.upper, context)
            return self._apply_between(
                self._PROCEDURES["Between"], value_result, lower_result, upper_result
            )

        raise NotImplementedError("No handler for Between expression")

    @visit.register
    def _visit_like(self, expr: Like, context: Optional[C] = None) -> R:
        """Visit a Like expression."""
        if hasattr(self, "visit_like"):
            return self.visit_like(expr, context)

        if "Like" in self._PROCEDURES:
            value_result = self.visit(expr.value, context)
            pattern_result = self.visit(expr.pattern, context)
            return self._apply_like(
                self._PROCEDURES["Like"], value_result, pattern_result
            )

        raise NotImplementedError("No handler for Like expression")

    @abstractmethod
    def visit_reference(self, expr: Reference, context: Optional[C] = None) -> R:
        """Visit a Reference expression."""
        pass

    @abstractmethod
    def visit_literal(self, expr: Literal, context: Optional[C] = None) -> R:
        """Visit a Literal expression."""
        pass

    def visit_binary_expression(
        self, expr: BinaryExpression, left: R, right: R, context: Optional[C] = None
    ) -> R:
        """Default fallback handler for binary expressions."""
        raise NotImplementedError(f"No handler for {type(expr).__name__}")

    def visit_unary_expression(
        self, expr: UnaryExpression, operand: R, context: Optional[C] = None
    ) -> R:
        """Default fallback handler for unary expressions."""
        raise NotImplementedError(f"No handler for {type(expr).__name__}")


class DisplayVisitor(ExpressionVisitor[Expression, str]):
    """
    Visitor implementation that formats expressions in standard infix notation.
    For example: "a = b AND c > d" instead of "(AND (= a b) (> c d))".
    """

    # Map all expression types to their string formatting procedures with infix notation
    _PROCEDURES = {
        # Binary operations with infix notation
        "Equal": lambda left, right: f"{left} = {right}",
        "NotEqual": lambda left, right: f"{left} <> {right}",
        "GreaterThan": lambda left, right: f"{left} > {right}",
        "LessThan": lambda left, right: f"{left} < {right}",
        "GreaterThanEqual": lambda left, right: f"{left} >= {right}",
        "LessThanEqual": lambda left, right: f"{left} <= {right}",
        "And": lambda left, right: f"({left} AND {right})",
        "Or": lambda left, right: f"({left} OR {right})",
        # Unary operations
        "Not": lambda operand: f"NOT ({operand})",
        "IsNull": lambda operand: f"({operand}) IS NULL",
        # Special operations
        "In": lambda value, values: f"{value} IN ({', '.join(values)})",
        "Between": lambda value, lower, upper: f"{value} BETWEEN {lower} AND {upper}",
        "Like": lambda value, pattern: f"{value} LIKE {pattern}",
    }

    def visit_reference(self, expr: Reference, context=None) -> str:
        """Format a field reference."""
        return expr.field

    def visit_literal(self, expr: Literal, context=None) -> str:
        """Format a literal value using its PyArrow representation."""
        return str(expr.value)

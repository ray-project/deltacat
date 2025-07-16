import pytest
import pyarrow as pa

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
from deltacat.storage.model.expression.visitor import DisplayVisitor, ExpressionVisitor


@pytest.fixture
def field_ref():
    return Reference("field1")


@pytest.fixture
def field_ref2():
    return Reference("field2")


@pytest.fixture
def literal_int():
    return Literal(pa.scalar(42))


@pytest.fixture
def literal_str():
    return Literal(pa.scalar("test"))


@pytest.fixture
def display_visitor():
    return DisplayVisitor()


class TestExpressionLibrary:
    """Test suite for the Deltacat expression library."""

    def test_reference_creation(self):
        ref = Reference("field1")
        assert ref.field == "field1"
        assert ref.index is None

    def test_reference_with_index(self):
        ref = Reference("field1", 0)
        assert ref.field == "field1"
        assert ref.index == 0

    def test_literal_creation(self):
        lit = Literal(pa.scalar(42))
        assert lit.value.as_py() == 42

    # Test the factory methods (.of)
    def test_factory_methods(self):
        # Reference.of
        ref = Reference.of("field1")
        assert ref.field == "field1"

        # Literal.of
        lit = Literal.of(42)
        assert lit.value.as_py() == 42

        # Equal.of with mixed types
        eq = Equal.of("field1", 42)
        assert isinstance(eq.left, Literal)
        assert isinstance(eq.right, Literal)
        assert eq.left.value.as_py() == "field1"
        assert eq.right.value.as_py() == 42

        # Not.of
        not_expr = Not.of(Equal.of("field1", 42))
        assert isinstance(not_expr.operand, Equal)

        # In.of
        in_expr = In.of("field1", [1, 2, 3])
        assert isinstance(in_expr.value, Literal)
        assert len(in_expr.values) == 3
        assert all(isinstance(v, Literal) for v in in_expr.values)

        # Between.of
        between_expr = Between.of("field1", 10, 20)
        assert isinstance(between_expr.value, Literal)
        assert between_expr.lower.value.as_py() == 10
        assert between_expr.upper.value.as_py() == 20

        # Like.of
        like_expr = Like.of("field1", "%test%")
        assert isinstance(like_expr.value, Literal)
        assert like_expr.pattern.value.as_py() == "%test%"

    # Test reference comparison helper methods
    def test_reference_comparison_helpers(self, field_ref):
        # Test eq, ne, gt, lt, ge, le methods
        eq_expr = field_ref.eq(42)
        assert isinstance(eq_expr, Equal)
        assert eq_expr.left == field_ref
        assert eq_expr.right.value.as_py() == 42

        ne_expr = field_ref.ne(42)
        assert isinstance(ne_expr, NotEqual)

        gt_expr = field_ref.gt(42)
        assert isinstance(gt_expr, GreaterThan)

        lt_expr = field_ref.lt(42)
        assert isinstance(lt_expr, LessThan)

        ge_expr = field_ref.ge(42)
        assert isinstance(ge_expr, GreaterThanEqual)

        le_expr = field_ref.le(42)
        assert isinstance(le_expr, LessThanEqual)

    # Test reference special operation helpers
    def test_reference_special_helpers(self, field_ref):
        # Test is_null, in_, between, like methods
        is_null_expr = field_ref.is_null()
        assert isinstance(is_null_expr, IsNull)
        assert is_null_expr.operand == field_ref

        in_expr = field_ref.in_([1, 2, 3])
        assert isinstance(in_expr, In)
        assert in_expr.value == field_ref
        assert len(in_expr.values) == 3
        assert in_expr.values[0].value.as_py() == 1

        between_expr = field_ref.between(10, 20)
        assert isinstance(between_expr, Between)
        assert between_expr.value == field_ref
        assert between_expr.lower.value.as_py() == 10
        assert between_expr.upper.value.as_py() == 20

        like_expr = field_ref.like("%test%")
        assert isinstance(like_expr, Like)
        assert like_expr.value == field_ref
        assert like_expr.pattern.value.as_py() == "%test%"

    # Test boolean expression helper methods
    def test_boolean_expression_helpers(self, field_ref):
        # Test and_, or_, not_ methods
        expr1 = field_ref.eq(42)
        expr2 = field_ref.gt(10)

        and_expr = expr1.and_(expr2)
        assert isinstance(and_expr, And)
        assert and_expr.left == expr1
        assert and_expr.right == expr2

        or_expr = expr1.or_(expr2)
        assert isinstance(or_expr, Or)
        assert or_expr.left == expr1
        assert or_expr.right == expr2

        not_expr = expr1.not_()
        assert isinstance(not_expr, Not)
        assert not_expr.operand == expr1

    # Test building complex expressions
    def test_complex_expression_building(self, field_ref, field_ref2):
        # Test building more complex expressions using method chaining
        expr = field_ref.eq(42).and_(field_ref2.gt(10)).or_(field_ref.is_null()).not_()

        assert isinstance(expr, Not)
        assert isinstance(expr.operand, Or)
        assert isinstance(expr.operand.left, And)
        assert isinstance(expr.operand.right, IsNull)

    # Test DisplayVisitor for different expression types
    def test_reference_display(self, field_ref, display_visitor):
        assert display_visitor.visit(field_ref) == "field1"

    def test_literal_display(self, literal_int, literal_str, display_visitor):
        assert display_visitor.visit(literal_int) == "42"
        assert display_visitor.visit(literal_str) == "test"

    def test_comparison_display(self, field_ref, literal_int, display_visitor):
        assert display_visitor.visit(Equal(field_ref, literal_int)) == "field1 = 42"
        assert display_visitor.visit(NotEqual(field_ref, literal_int)) == "field1 <> 42"
        assert (
            display_visitor.visit(GreaterThan(field_ref, literal_int)) == "field1 > 42"
        )
        assert display_visitor.visit(LessThan(field_ref, literal_int)) == "field1 < 42"
        assert (
            display_visitor.visit(GreaterThanEqual(field_ref, literal_int))
            == "field1 >= 42"
        )
        assert (
            display_visitor.visit(LessThanEqual(field_ref, literal_int))
            == "field1 <= 42"
        )

    def test_logical_operator_display(self, field_ref, literal_int, display_visitor):
        eq_expr = Equal(field_ref, literal_int)
        gt_expr = GreaterThan(field_ref, literal_int)

        assert (
            display_visitor.visit(And(eq_expr, gt_expr))
            == "(field1 = 42 AND field1 > 42)"
        )
        assert (
            display_visitor.visit(Or(eq_expr, gt_expr))
            == "(field1 = 42 OR field1 > 42)"
        )
        assert display_visitor.visit(Not(eq_expr)) == "NOT (field1 = 42)"

    def test_special_operator_display(self, field_ref, display_visitor):
        assert display_visitor.visit(IsNull(field_ref)) == "(field1) IS NULL"

        values = [Literal(pa.scalar(1)), Literal(pa.scalar(2)), Literal(pa.scalar(3))]
        assert display_visitor.visit(In(field_ref, values)) == "field1 IN (1, 2, 3)"

        lower = Literal(pa.scalar(10))
        upper = Literal(pa.scalar(20))
        assert (
            display_visitor.visit(Between(field_ref, lower, upper))
            == "field1 BETWEEN 10 AND 20"
        )

        pattern = Literal(pa.scalar("%test%"))
        assert display_visitor.visit(Like(field_ref, pattern)) == "field1 LIKE %test%"

    def test_complex_expression_display(self, field_ref, field_ref2, display_visitor):
        expr = field_ref.eq(42).and_(field_ref2.gt(10)).or_(field_ref.is_null()).not_()

        # Check that the DisplayVisitor correctly formats the complex expression
        assert (
            display_visitor.visit(expr)
            == "NOT (((field1 = 42 AND field2 > 10) OR (field1) IS NULL))"
        )

    # Test BinaryExpression with_ methods
    def test_binary_expression_with_methods(self, field_ref, field_ref2, literal_int):
        eq_expr = Equal(field_ref, literal_int)

        # Test with_left
        new_expr = eq_expr.with_left(field_ref2)
        assert isinstance(new_expr, Equal)
        assert new_expr.left == field_ref2
        assert new_expr.right == literal_int

        # Test with_right
        new_lit = Literal(pa.scalar(100))
        new_expr = eq_expr.with_right(new_lit)
        assert new_expr.left == field_ref
        assert new_expr.right == new_lit

    # Test __str__ method which uses DisplayVisitor
    def test_expression_str_method(self, field_ref, literal_int):
        eq_expr = Equal(field_ref, literal_int)
        assert str(eq_expr) == "field1 = 42"

    # Test proper parenthesization in complex expressions
    def test_nested_parentheses(self, field_ref, field_ref2, display_visitor):
        # Create a complex expression: (field1 = 1 AND field2 = 2) OR field2 = 3
        expr1 = Equal(field_ref, Literal(pa.scalar(1)))
        expr2 = Equal(field_ref2, Literal(pa.scalar(2)))
        expr3 = Equal(field_ref2, Literal(pa.scalar(3)))

        and_expr = And(expr1, expr2)
        or_expr = Or(and_expr, expr3)

        assert (
            display_visitor.visit(or_expr)
            == "((field1 = 1 AND field2 = 2) OR field2 = 3)"
        )

    # Test Literal comparison methods
    def test_literal_comparison_methods(self, literal_int):
        eq_expr = literal_int.eq("test")
        assert isinstance(eq_expr, Equal)
        assert eq_expr.left == literal_int
        assert eq_expr.right.value.as_py() == "test"

        ne_expr = literal_int.ne("test")
        assert isinstance(ne_expr, NotEqual)
        assert ne_expr.left == literal_int
        assert ne_expr.right.value.as_py() == "test"

    # Test a custom ExpressionVisitor implementation
    def test_custom_visitor(self, field_ref, literal_int):
        class CountingVisitor(ExpressionVisitor[None, int]):
            """Simple visitor that counts expression nodes"""

            def visit_reference(self, expr, context=None):
                return 1

            def visit_literal(self, expr, context=None):
                return 1

            def visit_binary_expression(self, expr, left, right, context=None):
                return left + right + 1

            def visit_unary_expression(self, expr, operand, context=None):
                return operand + 1

            def visit_in(self, expr, context=None):
                return 1 + len(expr.values) + 1  # value + all values + In operator

            def visit_between(self, expr, context=None):
                return 3  # value + lower + upper

            def visit_like(self, expr, context=None):
                return 2  # value + pattern

        visitor = CountingVisitor()

        # Count nodes in simple expressions
        assert visitor.visit(field_ref) == 1
        assert visitor.visit(literal_int) == 1
        assert visitor.visit(Equal(field_ref, literal_int)) == 3  # left + right + Equal

        # Count nodes in a more complex expression
        expr = field_ref.eq(42).and_(field_ref.gt(10))
        assert visitor.visit(expr) == 7  # (1+1+1) + (1+1+1) + 1

from daft.io.scan import ScanPushdowns
import pyarrow as pa
from typing import Callable, Dict
from daft.io.pushdowns import (
    Expr as DaftExpr,
    Literal as DaftLiteral,
    Reference as DaftReference,
    TermVisitor,
)

from deltacat.storage.model.expression import (
    Expression,
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
    IsNull,
)
from deltacat.storage.model.scan.push_down import PartitionFilter, Pushdown


def translate_pushdown(pushdown: ScanPushdowns) -> Pushdown:
    """
    Helper method to translate a Daft ScanPushdowns object into a Deltacat Pushdown.

    Args:
        pushdown: Daft ScanPushdowns object

    Returns:
        Pushdown: Deltacat Pushdown object with translated filters
    """
    translator = DaftToDeltacatExpressionTranslator()
    partition_filter = None

    if pushdown.predicate:
        predicate = translator.visit(pushdown.predicate, None)
        partition_filter = PartitionFilter.of(predicate)

    # TODO: translate other pushdown filters
    return Pushdown.of(
        row_filter=None,
        column_filter=None,
        partition_filter=partition_filter,
        limit=None,
    )


class DaftToDeltacatExpressionTranslator(TermVisitor[None, Expression]):
    """
    This visitor implementation traverses a Daft expression tree and produces
    an equivalent Deltacat expression tree for use in Deltacat's query pushdown
    system.
    """

    _PROCEDURES: Dict[str, Callable[..., Expression]] = {
        # Comparison predicates
        "=": Equal.of,
        "!=": NotEqual.of,
        "<": LessThan.of,
        ">": GreaterThan.of,
        "<=": LessThanEqual.of,
        ">=": GreaterThanEqual.of,
        # Logical predicates
        "and": And.of,
        "or": Or.of,
        "not": Not.of,
        # Special operations
        "is_null": IsNull.of,
    }

    def visit_reference(self, term: DaftReference, context: None) -> Expression:
        """
        Convert Daft Reference to Deltacat Reference.

        Args:
            term: A Daft Reference expression representing a field or column.
            context: Not used in this visitor implementation.

        Returns:
            DeltacatExpression: A Deltacat Reference expression for the same field.
        """
        return Reference(term.path)

    def visit_literal(self, term: DaftLiteral, context: None) -> Expression:
        """
        Convert Daft Literal to Deltacat Literal.

        Args:
            term: A Daft Literal expression representing a constant value.
            context: Not used in this visitor implementation.

        Returns:
            DeltacatExpression: A Deltacat Literal expression wrapping the same value as a PyArrow scalar.
        """
        return Literal(pa.scalar(term.value))

    def visit_expr(self, term: DaftExpr, context: None) -> Expression:
        """
        This method handles the translation of procedure calls (operations) from
        Daft to Deltacat, including special cases for IN, BETWEEN, and LIKE.

        Args:
            term: A Daft Expr expression representing an operation.
            context: Not used in this visitor implementation.

        Returns:
            DeltacatExpression: An equivalent Deltacat expression.

        Raises:
            ValueError: If the operation has an invalid number of arguments or
                if the operation is not supported by Deltacat.
        """
        proc = term.proc
        args = [self.visit(arg.term, context) for arg in term.args]

        if proc not in self._PROCEDURES:
            raise ValueError(f"Deltacat does not support procedure '{proc}'.")

        return self._PROCEDURES[proc](*args)

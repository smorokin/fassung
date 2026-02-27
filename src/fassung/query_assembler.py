from itertools import zip_longest
from string.templatelib import Template
from typing import Any, NamedTuple

from fassung.exceptions import UnsupportedTemplateError


class AssembledQuery(NamedTuple):
    """A parameterised query ready for asyncpg execution.

    Attributes:
        query: The SQL string with positional ``$N`` placeholders.
        args: The argument values corresponding to each placeholder.
    """

    query: str
    args: tuple[Any, ...]


class QueryAssembler:
    """Converts t-string templates into parameterised asyncpg queries."""

    def assemble(self, query: Template) -> AssembledQuery:
        """Assemble a Template into an [fassung.query_assembler.AssembledQuery][].

        Args:
            query: A t-string template containing SQL with interpolated values.

        Raises:
            UnsupportedTemplateError: If *query* is a plain string.
        """
        if isinstance(query, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise UnsupportedTemplateError("fassung does not support str as queries. Use Template query type instead.")  # pyright: ignore[reportUnreachable]
        assembled_query, args = self._assemble_recursive(query)
        if args:
            return AssembledQuery(assembled_query, args)
        return AssembledQuery(assembled_query, ())

    @staticmethod
    def _assemble_recursive(query: Template, counter_start: int = 0) -> tuple[str, tuple[Any, ...]]:
        """
        Recursively assemble a template query into an asyncpg query + arguments
        """
        placeholders: list[str] = []
        values: list[Any] = []
        counter = counter_start
        for value in query.values:
            if isinstance(value, Template):
                parsed_sub_query, sub_query_values = QueryAssembler._assemble_recursive(value, counter)
                if not parsed_sub_query.startswith(" "):  # add a space if necessary
                    parsed_sub_query = " " + parsed_sub_query
                placeholders.append(parsed_sub_query)
                values.extend(sub_query_values)
                counter += len(sub_query_values)
            else:
                placeholders.append(f"${counter + 1}")
                values.append(value)
                counter += 1

        # interleave strings and new placeholders. The first item in this list is always a string
        paired = zip_longest(query.strings, placeholders)
        flattened_and_filtered = [item for pair in paired for item in pair if item is not None]
        new_query = "".join(flattened_and_filtered)

        return new_query, tuple(values)

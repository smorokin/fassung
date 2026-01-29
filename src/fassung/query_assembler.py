from itertools import zip_longest
from string.templatelib import Template
from typing import Any, NamedTuple


class AssembledQuery(NamedTuple):
    query: str
    args: tuple[Any, ...]


class QueryAssembler:
    """
    Creates a asncpg query + arguments for methods like Pool.fetch or Pool.execute
    """

    def assemble(self, query: Template | str) -> AssembledQuery:
        """
        Assemble a template query into an asyncpg query + arguments
        """
        if isinstance(query, str):
            return AssembledQuery(query, ())
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

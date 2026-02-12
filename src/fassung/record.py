from collections.abc import Mapping

from asyncpg import Record


class MappedRecord(Record):
    """
    A Record that will be recognized as a mapping by Pydantic.
    """


# register MappedRecord as a mapping so pydantic can parse it without making Record a Mapping
Mapping.register(MappedRecord)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]

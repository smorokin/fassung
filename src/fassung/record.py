from collections.abc import Mapping

from asyncpg import Record


class MappedRecord(Record):
    """An asyncpg Record subclass registered as a Mapping.

    This allows Pydantic to validate record instances directly, without
    converting the base Record class itself into a Mapping.
    """


# register MappedRecord as a mapping so pydantic can parse it without making Record a Mapping
Mapping.register(MappedRecord)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]

from typing import Any, ClassVar, TypeVar

from asyncpg import Record
from pydantic import TypeAdapter

T = TypeVar("T")


class TypeParser:
    """
    Helper class for parsing asyncpg.Record objects into pydantic models. Caches type adapters for performance.
    """

    _type_adapters: ClassVar[dict[type, TypeAdapter[Any]]] = {}

    @classmethod
    def parse(cls, type_: type[T], value: Record | list[Record] | Any) -> T:
        """
        Parse a single value or Record into a pydantic model.
        """
        if type_ not in cls._type_adapters:
            cls._type_adapters[type_] = TypeAdapter(type_)
        return cls._type_adapters[type_].validate_python(value, by_alias=True)

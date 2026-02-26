from typing import Any, ClassVar, TypeVar

from asyncpg import Record
from pydantic import TypeAdapter

T = TypeVar("T")


class TypeParser:
    """Validates and converts raw asyncpg values into typed Python objects via Pydantic.

    Type adapters are cached so repeated conversions to the same target type
    skip adapter construction.
    """

    _type_adapters: ClassVar[dict[type, TypeAdapter[Any]]] = {}

    @classmethod
    def parse(cls, type_: type[T], value: Record | list[Record] | Any) -> T:
        """Validate *value* against *type_* and return the result.

        Args:
            type_: The target type to validate against.
            value: A raw value, Record, or list of Records to convert.
        """
        if type_ not in cls._type_adapters:
            cls._type_adapters[type_] = TypeAdapter(type_)
        return cls._type_adapters[type_].validate_python(value, by_alias=True)

from datetime import datetime
from typing import Any, TypeVar

import pytest
from pydantic import BaseModel

from fassung.type_parser import TypeParser

T = TypeVar("T")


class SomeModel(BaseModel):
    id: int
    field_int: int
    field_bool: bool
    field_float: float
    field_str: str
    field_datetime: datetime


@pytest.mark.parametrize(
    ("value", "type_", "expected"),
    [
        (1, int, 1),
        ("1", int, 1),
        ("true", bool, True),
        ("1.1", float, 1.1),
        ("2026-01-25 21:07:05", datetime, datetime(2026, 1, 25, 21, 7, 5)),
        (
            {  # we cannot create MappedRecord instances, so we use a dict instead
                "id": 1,
                "field_int": 1,
                "field_bool": True,
                "field_float": 1.1,
                "field_str": "some_string",
                "field_datetime": datetime(2026, 1, 25, 21, 7, 5),
            },
            SomeModel,
            SomeModel(
                id=1,
                field_int=1,
                field_bool=True,
                field_float=1.1,
                field_str="some_string",
                field_datetime=datetime(2026, 1, 25, 21, 7, 5),
            ),
        ),
    ],
)
def test_parse_simple_type(value: Any, type_: type[T], expected: T) -> None:
    parsed = TypeParser.parse(type_, value)
    assert isinstance(parsed, type_)
    assert parsed == expected

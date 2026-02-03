from pydantic import BaseModel

from fassung import Transaction


class SomeModel(BaseModel):
    id: int
    field_int: int
    field_bool: bool
    field_float: float
    field_str: str


async def test_transaction(transaction: Transaction) -> None:
    assert transaction


async def test_fixtures(transaction: Transaction) -> None:
    result = await transaction.fetch("SELECT * FROM students", SomeModel)
    assert len(result) == 2

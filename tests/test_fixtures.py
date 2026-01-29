from pydantic import BaseModel

from fassung.pool import Transaction


class SomeModel(BaseModel):
    id: int
    field_int: int
    field_bool: bool
    field_float: float
    field_str: str


async def test_transaction(transaction: Transaction) -> None:
    pass


async def test_fixtures(transaction: Transaction) -> None:
    result = await transaction.fetch("SELECT * FROM test", SomeModel)
    assert len(result) == 2

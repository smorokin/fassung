from fassung import Transaction
from tests.models import Student


async def test_transaction(transaction: Transaction) -> None:
    assert transaction


async def test_fixtures(transaction: Transaction) -> None:
    result = await transaction.fetch(Student, t"SELECT * FROM students")
    assert len(result) == 2

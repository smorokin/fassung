from fassung import Transaction
from tests.models import Student


async def test_cursor_iterator(transaction: Transaction) -> None:
    count = 0
    async for entry in transaction.cursor("SELECT * FROM students", Student):
        assert isinstance(entry, Student)
        assert entry.id in (1, 2)
        assert entry.name in ("John", "Jane")
        count += 1
    assert count == 2


async def test_cursor_iterator_is_iterable(transaction: Transaction) -> None:
    iterator = aiter(transaction.cursor("SELECT * FROM students", Student))
    count = 0
    async for entry in iterator:
        assert isinstance(entry, Student)
        assert entry.id in (1, 2)
        assert entry.name in ("John", "Jane")
        count += 1
    assert count == 2


async def test_cursor_fetch(transaction: Transaction) -> None:
    cursor = await transaction.cursor("SELECT * FROM students", Student)
    rows = await cursor.fetch(2)
    assert len(rows) == 2
    assert isinstance(rows[0], Student)
    assert rows[0].id in (1, 2)
    assert rows[0].name in ("John", "Jane")
    assert isinstance(rows[1], Student)
    assert rows[1].id in (1, 2)
    assert rows[1].name in ("John", "Jane")


async def test_cursor_fetchrow(transaction: Transaction) -> None:
    cursor = await transaction.cursor("SELECT * FROM students", Student)
    row = await cursor.fetchrow()
    assert isinstance(row, Student)
    assert row.id in (1, 2)
    assert row.name in ("John", "Jane")
    row = await cursor.fetchrow()
    assert isinstance(row, Student)
    assert row.id in (1, 2)
    assert row.name in ("John", "Jane")
    row = await cursor.fetchrow()
    assert row is None


async def test_cursor_forward(transaction: Transaction) -> None:
    cursor = await transaction.cursor("SELECT * FROM students", Student)
    skipped = await cursor.forward(1)
    assert skipped == 1
    row = await cursor.fetchrow()
    assert isinstance(row, Student)
    assert row.id in (1, 2)
    assert row.name in ("John", "Jane")
    row = await cursor.fetchrow()
    assert row is None

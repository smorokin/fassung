from fassung import Transaction
from tests.models import Student


async def test_cursor_iterator(transaction: Transaction) -> None:
    count = 0
    async for entry in transaction.cursor(Student, t"SELECT * FROM students"):
        assert isinstance(entry, Student)
        assert entry.id in (1, 2)
        assert entry.full_name in ("John Doe", "Jane Doe")
        count += 1
    assert count == 2


async def test_cursor_iterator_is_iterable(transaction: Transaction) -> None:
    iterator = aiter(transaction.cursor(Student, t"SELECT * FROM students"))
    count = 0
    async for entry in iterator:
        assert isinstance(entry, Student)
        assert entry.id in (1, 2)
        assert entry.full_name in ("John Doe", "Jane Doe")
        count += 1
    assert count == 2


async def test_cursor_fetch(transaction: Transaction) -> None:
    cursor = await transaction.cursor(Student, t"SELECT * FROM students")
    rows = await cursor.fetch(2)
    assert len(rows) == 2
    assert isinstance(rows[0], Student)
    assert rows[0].id in (1, 2)
    assert rows[0].full_name in ("John Doe", "Jane Doe")
    assert isinstance(rows[1], Student)
    assert rows[1].id in (1, 2)
    assert rows[1].full_name in ("John Doe", "Jane Doe")


async def test_cursor_fetchrow(transaction: Transaction) -> None:
    cursor = await transaction.cursor(Student, t"SELECT * FROM students")
    row = await cursor.fetchrow()
    assert isinstance(row, Student)
    assert row.id in (1, 2)
    assert row.full_name in ("John Doe", "Jane Doe")
    row = await cursor.fetchrow()
    assert isinstance(row, Student)
    assert row.id in (1, 2)
    assert row.full_name in ("John Doe", "Jane Doe")
    row = await cursor.fetchrow()
    assert row is None


async def test_cursor_forward(transaction: Transaction) -> None:
    cursor = await transaction.cursor(Student, t"SELECT * FROM students")
    skipped = await cursor.forward(1)
    assert skipped == 1
    row = await cursor.fetchrow()
    assert isinstance(row, Student)
    assert row.id in (1, 2)
    assert row.full_name in ("John Doe", "Jane Doe")
    row = await cursor.fetchrow()
    assert row is None

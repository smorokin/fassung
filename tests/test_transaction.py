from collections.abc import AsyncGenerator
from datetime import UTC, date, datetime
from string import Template

import pytest
from asyncpg import PostgresSyntaxError, UndefinedTableError

from fassung import Connection, Transaction, TransactionClosedError

from .models import Student


async def test_execute(transaction: Transaction) -> None:
    result = await transaction.execute(t"UPDATE students SET gpa = 1 WHERE id = 1")
    assert result == "UPDATE 1"

    val = await transaction.fetchval(int, t"SELECT gpa FROM students WHERE id = 1")
    assert val == 1


async def test_cursor(transaction: Transaction) -> None:
    rows: list[Student] = []
    async for entry in transaction.cursor(Student, t"SELECT * FROM students ORDER BY id"):
        rows.append(entry)

    assert len(rows) == 2
    assert rows[0].id == 1
    assert rows[1].id == 2


async def test_fetch(transaction: Transaction) -> None:
    rows = await transaction.fetch(Student, t"SELECT * FROM students ORDER BY id")
    assert len(rows) == 2
    assert isinstance(rows[0], Student)
    assert rows[0].id == 1
    assert rows[1].id == 2


async def test_fetchval(transaction: Transaction) -> None:
    val = await transaction.fetchval(str, t"SELECT full_name FROM students WHERE id = 1")
    assert val == "Jane Doe"


async def test_fetchrow(transaction: Transaction) -> None:
    row = await transaction.fetchrow(Student, t"SELECT * FROM students WHERE id = 2")
    assert row is not None
    assert row.id == 2
    assert row.full_name == "John Doe"

    row_none = await transaction.fetchrow(Student, t"SELECT * FROM students WHERE id = 999")
    assert row_none is None


@pytest.fixture
async def connection_with_temporary_table(connection: Connection) -> AsyncGenerator[Connection]:
    async with connection as transaction:
        _ = await transaction.execute(t"CREATE TEMPORARY TABLE test_table (id INT, name TEXT)")
    try:
        yield connection
    finally:
        async with connection as transaction:
            _ = await transaction.execute(t"DROP TABLE test_table")


async def test_commit(connection_with_temporary_table: Connection) -> None:
    id = 123
    name = "Walter"
    async with connection_with_temporary_table as transaction:
        _ = await transaction.execute(t"INSERT INTO test_table (id, name) VALUES ({id}, {name})")

    val = await connection_with_temporary_table.fetchval(str, t"SELECT name FROM test_table WHERE id = {id}")
    assert val == name


async def test_manual_rollback(connection_with_temporary_table: Connection) -> None:
    # Verify that manual rollback works
    async with connection_with_temporary_table as transaction:
        _ = await transaction.execute(t"INSERT INTO test_table (id, name) VALUES (1, 'Walter')")

        transaction.mark_for_rollback()
        with pytest.raises(TransactionClosedError):  # inside of context manager
            _ = await transaction.execute(t"SELECT * FROM test_table")

    with pytest.raises(TransactionClosedError):  # outside of context manager
        _ = await transaction.execute(t"SELECT * FROM test_table")

    row = await connection_with_temporary_table.fetchrow(str, t"SELECT name FROM test_table WHERE id = 1")
    assert row is None


async def test_rollback_on_error(connection: Connection) -> None:
    # Verify that rollback on error works
    try:
        async with connection as transaction:
            _ = await transaction.execute(t"CREATE TEMPORARY TABLE test_commit (id int)")
            _ = await transaction.execute(t"INSERT INTO test_commit (id) VALUES (1)")

            _ = await transaction.execute(t"asdf")
    except PostgresSyntaxError:
        pass

    with pytest.raises(UndefinedTableError):
        _ = await connection.execute(t"SELECT * FROM test_commit")


@pytest.mark.parametrize(
    ("where_query", "order_query", "expected_ids"), [(t"", t"ORDER BY id DESC", [2, 1]), (t"WHERE id = 1", t"", [1])]
)
async def test_nested_fetch(
    transaction: Transaction, where_query: Template, order_query: Template, expected_ids: list[int]
) -> None:
    rows = await transaction.fetch(Student, t"SELECT * FROM students {where_query} {order_query}")
    assert len(rows) == len(expected_ids)
    assert isinstance(rows[0], Student)
    assert [r.id for r in rows] == expected_ids


async def test_sql_injection_prevention(transaction: Transaction) -> None:
    full_name = "Robert'); DROP TABLE students; -- Smith"
    id = 3
    email = "bobby.tables@example.com"
    birth_date = date(2002, 5, 14)
    major = "Physics"
    gpa = 3.9
    is_active = True
    enrolled_at = datetime(2023, 9, 1, 9, 0, 0, tzinfo=UTC)
    last_seen_at = datetime(2024, 2, 1, 10, 30, 0, tzinfo=UTC)
    _ = await transaction.execute(
        t"""
        INSERT INTO students (
            id,
            full_name,
            email,
            birth_date,
            major,
            gpa,
            is_active,
            enrolled_at,
            last_seen_at
        ) VALUES (
            {id},
            {full_name},
            {email},
            {birth_date},
            {major},
            {gpa},
            {is_active},
            {enrolled_at},
            {last_seen_at}
        )
        """
    )
    count = await transaction.fetchval(int, t"SELECT COUNT(*) FROM students")
    assert count == 3, "Bobby Tables did it again!"

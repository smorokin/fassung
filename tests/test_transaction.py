from collections.abc import AsyncGenerator
from string import Template

import pytest
from asyncpg import PostgresSyntaxError, UndefinedTableError

from fassung import Connection, Transaction, TransactionClosedError

from .models import Student


async def test_execute(transaction: Transaction) -> None:
    result = await transaction.execute("UPDATE students SET field_int = 100 WHERE id = 1")
    assert result == "UPDATE 1"

    val = await transaction.fetchval("SELECT field_int FROM students WHERE id = 1", int)
    assert val == 100


async def test_cursor(transaction: Transaction) -> None:
    rows = []
    async for entry in transaction.cursor("SELECT * FROM students ORDER BY id", Student):
        rows.append(entry)

    assert len(rows) == 2
    assert rows[0].id == 1
    assert rows[1].id == 2


async def test_fetch(transaction: Transaction) -> None:
    rows = await transaction.fetch("SELECT * FROM students ORDER BY id", Student)
    assert len(rows) == 2
    assert isinstance(rows[0], Student)
    assert rows[0].id == 1
    assert rows[1].id == 2


async def test_fetchval(transaction: Transaction) -> None:
    val = await transaction.fetchval("SELECT field_str FROM students WHERE id = 1", str)
    assert val == "some_string"


async def test_fetchrow(transaction: Transaction) -> None:
    row = await transaction.fetchrow("SELECT * FROM students WHERE id = 1", Student)
    assert row is not None
    assert row.id == 1
    assert row.field_str == "some_string"

    row_none = await transaction.fetchrow("SELECT * FROM students WHERE id = 999", Student)
    assert row_none is None


@pytest.fixture
async def connection_with_temporary_table(connection: Connection) -> AsyncGenerator[Connection]:
    async with connection as transaction:
        _ = await transaction.execute("CREATE TEMPORARY TABLE test_table (id INT, name TEXT)")
    try:
        yield connection
    finally:
        async with connection as transaction:
            _ = await transaction.execute("DROP TABLE test_table")


async def test_commit(connection_with_temporary_table: Connection) -> None:
    id = 123
    name = "Walter"
    async with connection_with_temporary_table as transaction:
        _ = await transaction.execute(t"INSERT INTO test_table (id, name) VALUES ({id}, {name})")

    val = await connection_with_temporary_table.fetchval(t"SELECT name FROM test_table WHERE id = {id}", str)
    assert val == name


async def test_manual_rollback(connection_with_temporary_table: Connection) -> None:
    # Verify that manual rollback works
    async with connection_with_temporary_table as transaction:
        _ = await transaction.execute("INSERT INTO test_table (id, name) VALUES (1, 'Walter')")

        transaction.mark_for_rollback()
        with pytest.raises(TransactionClosedError):  # inside of context manager
            _ = await transaction.execute("SELECT * FROM test_table")

    with pytest.raises(TransactionClosedError):  # outside of context manager
        _ = await transaction.execute("SELECT * FROM test_table")

    row = await connection_with_temporary_table.fetchrow("SELECT name FROM test_table WHERE id = 1", str)
    assert row is None


async def test_rollback_on_error(connection: Connection) -> None:
    # Verify that rollback on error works
    try:
        async with connection as transaction:
            _ = await transaction.execute("CREATE TEMPORARY TABLE test_commit (id int)")
            _ = await transaction.execute("INSERT INTO test_commit (id) VALUES (1)")

            _ = await transaction.execute("asdf")
    except PostgresSyntaxError:
        pass

    with pytest.raises(UndefinedTableError):
        _ = await connection.execute("SELECT * FROM test_commit")


@pytest.mark.parametrize(
    ("where_query", "order_query", "expected_ids"), [(t"", t"ORDER BY id DESC", [2, 1]), (t"WHERE id = 1", t"", [1])]
)
async def test_nested_fetch(
    transaction: Transaction, where_query: Template, order_query: Template, expected_ids: list[int]
) -> None:
    rows = await transaction.fetch(t"SELECT * FROM students {where_query} {order_query}", Student)
    assert len(rows) == len(expected_ids)
    assert isinstance(rows[0], Student)
    assert [r.id for r in rows] == expected_ids


async def test_sql_injection_prevention(transaction: Transaction) -> None:
    name = "Robert'); DROP TABLE students; --"
    id = 3
    value_int = 1
    value_bool = True
    value_float = 1.0
    value_str = "some_string"
    _ = await transaction.execute(
        t"""
        INSERT INTO students (id, field_int, field_bool, field_float, field_str, name)
        VALUES ({id}, {value_int}, {value_bool}, {value_float}, {value_str}, {name})
        """
    )
    count = await transaction.fetchval("SELECT COUNT(*) FROM students", int)
    assert count == 3, "Bobby Tables did it again!"

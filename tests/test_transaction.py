from string import Template

import pytest
from asyncpg import PostgresSyntaxError, UndefinedTableError
from pydantic import BaseModel

from fassung.pool import Connection, Transaction, TransactionClosedError


class Row(BaseModel):
    id: int
    field_int: int
    field_bool: bool
    field_float: float
    field_str: str


async def test_execute(transaction: Transaction) -> None:
    result = await transaction.execute("UPDATE test SET field_int = 100 WHERE id = 1")
    assert result == "UPDATE 1"

    val = await transaction.fetchval("SELECT field_int FROM test WHERE id = 1", int)
    assert val == 100


async def test_cursor(transaction: Transaction) -> None:
    rows = []
    async for entry in transaction.cursor("SELECT * FROM test ORDER BY id", Row):
        rows.append(entry)

    assert len(rows) == 2
    assert rows[0].id == 1
    assert rows[1].id == 2


async def test_fetch(transaction: Transaction) -> None:
    rows = await transaction.fetch("SELECT * FROM test ORDER BY id", Row)
    assert len(rows) == 2
    assert isinstance(rows[0], Row)
    assert rows[0].id == 1
    assert rows[1].id == 2


async def test_fetchval(transaction: Transaction) -> None:
    val = await transaction.fetchval("SELECT field_str FROM test WHERE id = 1", str)
    assert val == "some_string"


async def test_fetchrow(transaction: Transaction) -> None:
    row = await transaction.fetchrow("SELECT * FROM test WHERE id = 1", Row)
    assert row is not None
    assert row.id == 1
    assert row.field_str == "some_string"

    row_none = await transaction.fetchrow("SELECT * FROM test WHERE id = 999", Row)
    assert row_none is None


async def test_commit(connection: Connection) -> None:
    id = 123
    value = "some_value"
    async with connection as transaction:
        _ = await transaction.execute("CREATE TEMPORARY TABLE test_commit (id int, field TEXT)")
        _ = await transaction.execute(t"INSERT INTO test_commit (id, field) VALUES ({id}, {value})")

    val = await connection.fetchval(t"SELECT field FROM test_commit WHERE id = {id}", str)
    assert val == value


async def test_manual_rollback(connection: Connection) -> None:
    # Verify that manual rollback works
    async with connection as transaction:
        _ = await transaction.execute("CREATE TEMPORARY TABLE test_commit (id int)")
        _ = await transaction.execute("INSERT INTO test_commit (id) VALUES (1)")

        transaction.mark_for_rollback()
        with pytest.raises(TransactionClosedError):  # inside of context manager
            _ = await transaction.execute("SELECT * FROM test_commit")

    with pytest.raises(TransactionClosedError):  # outside of context manager
        _ = await transaction.execute("SELECT * FROM test_commit")

    with pytest.raises(UndefinedTableError):
        _ = await connection.execute("SELECT * FROM test_commit")


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
    rows = await transaction.fetch(t"SELECT * FROM test {where_query} {order_query}", Row)
    assert len(rows) == len(expected_ids)
    assert isinstance(rows[0], Row)
    assert [r.id for r in rows] == expected_ids

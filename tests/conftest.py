from collections.abc import AsyncGenerator
from os import environ

import pytest

from fassung.pool import Connection, Pool, Transaction


@pytest.fixture
async def connection_string() -> str:
    return f"postgresql://{environ['POSTGRES_USER']}:{environ['POSTGRES_PASSWORD']}@localhost:5432/{environ['POSTGRES_DB']}"


@pytest.fixture
async def pool(connection_string: str) -> Pool:
    return await Pool.from_connection_string(connection_string)


async def _add_test_table_and_data(connection: Connection | Transaction) -> None:
    _ = await connection.execute("""
    CREATE TABLE IF NOT EXISTS test (
        id INT PRIMARY KEY,
        field_int INT,
        field_bool BOOLEAN,
        field_float FLOAT,
        field_str TEXT
    );""")
    _ = await connection.execute(
        """INSERT INTO test (id, field_int, field_bool, field_float, field_str)
        VALUES (1, 1, true, 42.0, 'some_string');"""
    )
    _ = await connection.execute(
        """INSERT INTO test (id, field_int, field_bool, field_float, field_str)
        VALUES (2, 5, false, 42.0, 'some_other_string');"""
    )


@pytest.fixture
async def connection(pool: Pool) -> AsyncGenerator[Connection]:
    async with pool.acquire() as connection:
        yield connection


@pytest.fixture
async def transaction(connection: Connection) -> AsyncGenerator[Transaction]:
    async with connection as transaction:
        await _add_test_table_and_data(transaction)
        yield transaction
        transaction.mark_for_rollback()

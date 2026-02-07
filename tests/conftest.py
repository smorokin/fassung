from collections.abc import AsyncGenerator
from os import environ

import pytest

from fassung import Connection, Pool, Transaction


@pytest.fixture(scope="session")
def connection_string() -> str:
    return f"postgresql://{environ['POSTGRES_USER']}:{environ['POSTGRES_PASSWORD']}@localhost:5432/{environ['POSTGRES_DB']}"


@pytest.fixture(scope="session")
async def pool(connection_string: str) -> Pool:
    return await Pool.from_connection_string(connection_string)


async def _add_test_table_and_data(connection: Connection | Transaction) -> None:
    _ = await connection.execute(t"""
    CREATE TABLE IF NOT EXISTS students (
        id INT PRIMARY KEY,
        full_name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        birth_date DATE,
        major TEXT NOT NULL,
        gpa FLOAT,
        is_active BOOLEAN,
        enrolled_at TIMESTAMPTZ NOT NULL,
        last_seen_at TIMESTAMPTZ
    );""")
    _ = await connection.execute(
        t"""INSERT INTO students (
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
            1,
            'Jane Doe',
            'jane@example.edu',
            '2002-05-14',
            'Physics',
            3.9,
            true,
            '2023-09-01 09:00:00+00',
            '2024-02-01 10:30:00+00'
        );"""
    )
    _ = await connection.execute(
        t"""INSERT INTO students (
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
            2,
            'John Doe',
            'john@example.edu',
            '2001-11-22',
            'Mathematics',
            2.8,
            false,
            '2022-01-15 14:30:00+00',
            NULL
        );"""
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

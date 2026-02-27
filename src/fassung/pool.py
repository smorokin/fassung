from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

from asyncpg import create_pool
from asyncpg.pool import Pool as AsyncpgPool

from fassung import Connection
from fassung.query_assembler import QueryAssembler
from fassung.record import MappedRecord


class Pool:
    """
    A connection pool for managing database connections.
    """

    def __init__(self, pool: AsyncpgPool, query_assembler: QueryAssembler | None = None) -> None:
        """
        Initialize a new connection pool.

        Args:
            pool: The asyncpg pool to use.
            query_assembler: The [fassung.query_assembler.QueryAssembler][] to use.
        """
        self._pool: AsyncpgPool = pool
        self.query_assembler: QueryAssembler = query_assembler or QueryAssembler()

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[Connection]:
        """
        Acquire a connection from the pool.

        Yields:
            A [fassung.connection.Connection][] to the database.
        """
        async with self._pool.acquire() as connection:
            yield Connection(connection, self.query_assembler)

    @staticmethod
    async def from_connection_string(
        connection_string: str,
        *,
        min_size: int = 10,
        max_size: int = 10,
        max_queries: int = 50000,
        max_inactive_connection_lifetime: float = 300.0,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        passfile: str | None = None,
        database: str | None = None,
        query_assembler: QueryAssembler | None = None,
    ) -> Pool:
        """
        Convinience function to create a new connection pool.

        Args:
            connection_string: The connection string to use.
            min_size: The minimum number of connections in the pool.
            max_size: The maximum number of connections in the pool.
            max_queries: The maximum number of queries to execute before closing a connection.
            max_inactive_connection_lifetime: The maximum time a connection can be inactive before being closed.
            host: The host to connect to.
            port: The port to connect to.
            user: The user to connect as.
            password: The password to use for authentication.
            passfile: The path to the password file.
            database: The database to connect to.
            query_assembler: The [fassung.query_assembler.QueryAssembler][] to use.
        """
        query_assembler = query_assembler or QueryAssembler()
        asyncpg_pool = cast(
            AsyncpgPool,
            await create_pool(
                connection_string,
                min_size=min_size,
                max_size=max_size,
                max_queries=max_queries,
                max_inactive_connection_lifetime=max_inactive_connection_lifetime,
                host=host,
                port=port,
                user=user,
                password=password,
                passfile=passfile,
                database=database,
                record_class=MappedRecord,
            ),
        )
        return Pool(asyncpg_pool, query_assembler)

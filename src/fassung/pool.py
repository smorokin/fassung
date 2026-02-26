from __future__ import annotations

from contextlib import asynccontextmanager
from typing import cast

from asyncpg import create_pool
from asyncpg.pool import Pool as AsyncpgPool

from fassung import Connection
from fassung.query_assembler import QueryAssembler
from fassung.record import MappedRecord


class Pool:
    def __init__(self, pool: AsyncpgPool, query_assembler: QueryAssembler | None = None) -> None:
        self._pool: AsyncpgPool = pool
        self.query_assembler: QueryAssembler = query_assembler or QueryAssembler()

    @asynccontextmanager
    async def acquire(self):
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

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from types import TracebackType
from typing import override

from asyncpg.pool import Pool as AsyncpgPool

from fassung.connection import Connection
from fassung.query_assembler import QueryAssembler


class Context(AbstractAsyncContextManager[Connection]):
    def __init__(self, pool: AsyncpgPool, query_assembler: QueryAssembler) -> None:
        self._pool: AsyncpgPool = pool
        self._query_assembler: QueryAssembler = query_assembler
        self._connection: Connection | None = None

    @override
    async def __aenter__(self) -> Connection:
        self._connection = Connection(await self._pool.acquire(), self._query_assembler)
        return self._connection

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._connection is None:
            raise RuntimeError("Connection is not initialized")  # pragma: no cover
        await self._pool.release(self._connection._connection)

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from enum import StrEnum
from string.templatelib import Template
from types import TracebackType
from typing import Self, TypeVar, cast, override

from asyncpg import create_pool
from asyncpg.cursor import (
    Cursor as AsyncpgCursor,
    CursorFactory as AsyncpgCursorFactory,
    CursorIterator as AsyncpgCursorIterator,
)
from asyncpg.pool import Pool as AsyncpgPool, PoolConnectionProxy
from asyncpg.transaction import Transaction as AsyncpgTransaction

from fassung.query_assembler import QueryAssembler
from fassung.record import MappedRecord
from fassung.type_parser import TypeParser

T = TypeVar("T")


class TransactionClosedError(Exception):
    pass


class TransactionStatus(StrEnum):
    STARTED = "started"
    COMMITTED = "committed"
    MARKED_FOR_ROLLBACK = "marked_for_rollback"
    ROLLED_BACK = "rolled_back"


class CursorIterator[T]:
    def __init__(self, cursor: AsyncpgCursorIterator, type_: type[T]) -> None:
        self._iterator: AsyncpgCursorIterator = cursor
        self._type: type[T] = type_

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        raw_record = await self._iterator.__anext__()
        return TypeParser.parse(self._type, raw_record)


class Cursor[T]:
    def __init__(self, cursor: AsyncpgCursor, type_: type[T]) -> None:
        self._cursor: AsyncpgCursor = cursor
        self._type: type[T] = type_

    async def fetch(self, n: int, *, timeout=None) -> list[T]:
        raw_list = await self._cursor.fetch(n, timeout=timeout)
        return TypeParser.parse(list[self._type], raw_list)

    async def fetchrow(self, *, timeout=None) -> T | None:
        raw_record = await self._cursor.fetchrow(timeout=timeout)
        if not raw_record:
            return None
        return TypeParser.parse(self._type, raw_record)

    async def forward(self, n: int, *, timeout=None) -> int:
        return await self._cursor.forward(n, timeout=timeout)


class CursorFactory[T]:
    def __init__(self, cursor_factory: AsyncpgCursorFactory, type_: type[T]) -> None:
        self._cursor_factory: AsyncpgCursorFactory = cursor_factory
        self._type: type[T] = type_

    def __aiter__(self) -> CursorIterator[T]:
        iterator = self._cursor_factory.__aiter__()
        return CursorIterator(iterator, self._type)

    async def __await__(self) -> Cursor[T]:
        cursor = await self._cursor_factory.__await__()
        return Cursor(cursor, self._type)


class Transaction:
    def __init__(self, connection: Connection, transaction: AsyncpgTransaction) -> None:
        self._connection: Connection = connection
        self._transaction: AsyncpgTransaction = transaction
        self._status: TransactionStatus = TransactionStatus.STARTED

    async def execute(self, query: str | Template) -> str:
        self._check_status()
        return await self._connection.execute(query)

    def cursor(
        self, query: str | Template, type_: type[T], *, prefetch: int | None = None, timeout: float | None = None
    ) -> CursorFactory[T]:
        self._check_status()
        return self._connection.cursor(query=query, type_=type_, prefetch=prefetch, timeout=timeout)

    async def fetch(self, query: str | Template, type_: type[T], *, timeout: float | None = None) -> list[T]:
        self._check_status()
        return await self._connection.fetch(query=query, type_=type_, timeout=timeout)

    async def fetchval(
        self, query: str | Template, type_: type[T], column: int = 0, *, timeout: float | None = None
    ) -> T:
        self._check_status()
        return await self._connection.fetchval(query=query, type_=type_, column=column, timeout=timeout)

    async def fetchrow(self, query: str | Template, type_: type[T], *, timeout: float | None = None) -> T | None:
        self._check_status()
        return await self._connection.fetchrow(query=query, type_=type_, timeout=timeout)

    async def _rollback(self) -> None:
        await self._transaction.rollback()
        self._status = TransactionStatus.ROLLED_BACK

    async def _commit(self) -> None:
        await self._transaction.commit()
        self._status = TransactionStatus.COMMITTED

    def mark_for_rollback(self) -> None:
        """
        Abort the transaction and mark it for rollback. The transaction cannot be used
        after this method is called.
        """
        self._status = TransactionStatus.MARKED_FOR_ROLLBACK

    def _check_status(self) -> None:
        if self._status != TransactionStatus.STARTED:
            raise TransactionClosedError(f"Transaction is closed: {self._status}")


class Connection(AbstractAsyncContextManager[Transaction]):
    def __init__(self, connection: PoolConnectionProxy, query_assembler: QueryAssembler) -> None:
        self._connection: PoolConnectionProxy = connection
        self._query_assembler: QueryAssembler = query_assembler
        self._transaction: Transaction | None = None

    async def execute(self, query: str | Template, *, timeout: float | None = None) -> str:
        assembled = self._query_assembler.assemble(query)
        return await self._connection.execute(assembled.query, *assembled.args, timeout=timeout)

    def cursor(
        self, query: str | Template, type_: type[T], *, prefetch: int | None = None, timeout: float | None = None
    ) -> CursorFactory[T]:
        assembled = self._query_assembler.assemble(query)
        return CursorFactory(
            cursor_factory=self._connection.cursor(
                assembled.query, *assembled.args, prefetch=prefetch, timeout=timeout
            ),
            type_=type_,
        )

    async def fetch(self, query: str | Template, type_: type[T], *, timeout: float | None = None) -> list[T]:
        assembled = self._query_assembler.assemble(query)
        raw_list = await self._connection.fetch(assembled.query, *assembled.args, timeout=timeout)
        return TypeParser.parse(list[type_], raw_list)

    async def fetchval(
        self, query: str | Template, type_: type[T], column: int = 0, *, timeout: float | None = None
    ) -> T:
        assembled = self._query_assembler.assemble(query)
        raw_value = await self._connection.fetchval(assembled.query, *assembled.args, column=column, timeout=timeout)
        return TypeParser.parse(type_, raw_value)

    async def fetchrow(self, query: str | Template, type_: type[T], *, timeout: float | None = None) -> T | None:
        assembled = self._query_assembler.assemble(query)
        raw_row = await self._connection.fetchrow(assembled.query, *assembled.args, timeout=timeout)
        if raw_row is None:
            return None
        return TypeParser.parse(type_, raw_row)

    @override
    async def __aenter__(self) -> Transaction:
        transaction = self._connection.transaction()
        await transaction.start()
        self._transaction = Transaction(self, transaction)
        return self._transaction

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._transaction is None:
            raise RuntimeError("Transaction is not initialized")  # pragma: no cover

        if exc_val is None:
            if self._transaction._status == TransactionStatus.MARKED_FOR_ROLLBACK:
                await self._transaction._rollback()
            else:
                await self._transaction._commit()
        else:
            await self._transaction._rollback()
            raise exc_val


class Context(AbstractAsyncContextManager[Connection]):
    def __init__(self, pool: AsyncpgPool, query_assembler: QueryAssembler) -> None:
        self._pool = pool
        self._query_assembler = query_assembler
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


class Pool:
    def __init__(self, pool: AsyncpgPool, query_assembler: QueryAssembler | None = None) -> None:
        self._pool = pool
        self.query_assembler = query_assembler or QueryAssembler()

    def acquire(self) -> Context:
        return Context(self._pool, self.query_assembler)

    @staticmethod
    async def from_connection_string(connection_string: str) -> Pool:
        asyncpg_pool = cast(AsyncpgPool, await create_pool(connection_string, record_class=MappedRecord))
        return Pool(asyncpg_pool, QueryAssembler())

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from enum import StrEnum
from string.templatelib import Template
from types import TracebackType
from typing import TypeVar, override

from asyncpg.pool import PoolConnectionProxy
from asyncpg.transaction import Transaction as AsyncpgTransaction

from fassung.cursor import CursorFactory
from fassung.exceptions import TransactionClosedError
from fassung.query_assembler import QueryAssembler
from fassung.type_parser import TypeParser

T = TypeVar("T")


class TransactionStatus(StrEnum):
    STARTED = "started"
    COMMITTED = "committed"
    MARKED_FOR_ROLLBACK = "marked_for_rollback"
    ROLLED_BACK = "rolled_back"


class Transaction:
    def __init__(self, connection: Connection, transaction: AsyncpgTransaction) -> None:
        self._connection: Connection = connection
        self._transaction: AsyncpgTransaction = transaction
        self._status: TransactionStatus = TransactionStatus.STARTED

    async def execute(self, query: Template) -> str:
        self._check_status()
        return await self._connection.execute(query)

    def cursor(
        self, type_: type[T], query: Template, *, prefetch: int | None = None, timeout: float | None = None
    ) -> CursorFactory[T]:
        self._check_status()
        return self._connection.cursor(type_=type_, query=query, prefetch=prefetch, timeout=timeout)

    async def fetch(self, type_: type[T], query: Template, *, timeout: float | None = None) -> list[T]:
        self._check_status()
        return await self._connection.fetch(type_=type_, query=query, timeout=timeout)

    async def fetchval(self, type_: type[T], query: Template, column: int = 0, *, timeout: float | None = None) -> T:
        self._check_status()
        return await self._connection.fetchval(type_=type_, query=query, column=column, timeout=timeout)

    async def fetchrow(self, type_: type[T], query: Template, *, timeout: float | None = None) -> T | None:
        self._check_status()
        return await self._connection.fetchrow(type_=type_, query=query, timeout=timeout)

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
            raise TransactionClosedError(f"Transaction is not in started status: {self._status}")


class Connection(AbstractAsyncContextManager[Transaction]):
    def __init__(self, connection: PoolConnectionProxy, query_assembler: QueryAssembler) -> None:
        self._connection: PoolConnectionProxy = connection
        self._query_assembler: QueryAssembler = query_assembler
        self._transaction: Transaction | None = None

    async def execute(self, query: Template, *, timeout: float | None = None) -> str:
        assembled = self._query_assembler.assemble(query)
        return await self._connection.execute(assembled.query, *assembled.args, timeout=timeout)

    def cursor(
        self, type_: type[T], query: Template, *, prefetch: int | None = None, timeout: float | None = None
    ) -> CursorFactory[T]:
        assembled = self._query_assembler.assemble(query)
        return CursorFactory(
            cursor_factory=self._connection.cursor(
                assembled.query, *assembled.args, prefetch=prefetch, timeout=timeout
            ),
            type_=type_,
        )

    async def fetch(self, type_: type[T], query: Template, *, timeout: float | None = None) -> list[T]:
        assembled = self._query_assembler.assemble(query)
        raw_list = await self._connection.fetch(assembled.query, *assembled.args, timeout=timeout)
        return TypeParser.parse(list[type_], raw_list)

    async def fetchval(self, type_: type[T], query: Template, column: int = 0, *, timeout: float | None = None) -> T:
        assembled = self._query_assembler.assemble(query)
        raw_value = await self._connection.fetchval(assembled.query, *assembled.args, column=column, timeout=timeout)
        return TypeParser.parse(type_, raw_value)

    async def fetchrow(self, type_: type[T], query: Template, *, timeout: float | None = None) -> T | None:
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

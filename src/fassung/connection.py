from __future__ import annotations

from collections.abc import Awaitable
from contextlib import AbstractAsyncContextManager
from enum import StrEnum
from inspect import iscoroutinefunction
from string.templatelib import Template
from types import TracebackType
from typing import Any, Protocol, TypeVar, override

from asyncpg.connection import Connection as AsyncpgConnection
from asyncpg.pool import PoolConnectionProxy
from asyncpg.transaction import Transaction as AsyncpgTransaction

from fassung.cursor import CursorFactory
from fassung.exceptions import TransactionClosedError
from fassung.query_assembler import QueryAssembler
from fassung.type_parser import TypeParser
from fassung.types import Listener

T = TypeVar("T")


class _InnerListener(Protocol):
    def __call__(
        self,
        con_ref: AsyncpgConnection | PoolConnectionProxy,
        pid: int,
        channel: str,
        payload: object,
        /,
    ) -> Awaitable[None] | None: ...


class TransactionStatus(StrEnum):
    STARTED = "started"
    COMMITTED = "committed"
    MARKED_FOR_ROLLBACK = "marked_for_rollback"
    ROLLED_BACK = "rolled_back"


class Transaction:
    def __init__(self, connection: Connection, transaction: AsyncpgTransaction) -> None:
        self._connection: Connection = connection
        self._transaction: AsyncpgTransaction = transaction
        self.status: TransactionStatus = TransactionStatus.STARTED

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

    async def rollback(self) -> None:
        await self._transaction.rollback()
        self.status = TransactionStatus.ROLLED_BACK

    async def commit(self) -> None:
        await self._transaction.commit()
        self.status = TransactionStatus.COMMITTED

    def mark_for_rollback(self) -> None:
        """
        Abort the transaction and mark it for rollback. The transaction cannot be used
        after this method is called.
        """
        self.status = TransactionStatus.MARKED_FOR_ROLLBACK

    def _check_status(self) -> None:
        if self.status != TransactionStatus.STARTED:
            raise TransactionClosedError(f"Transaction is not in started status: {self.status}")


class Connection(AbstractAsyncContextManager[Transaction]):
    def __init__(self, connection: AsyncpgConnection | PoolConnectionProxy, query_assembler: QueryAssembler) -> None:
        self._connection: AsyncpgConnection | PoolConnectionProxy = connection
        self._query_assembler: QueryAssembler = query_assembler
        self._transaction: Transaction | None = None
        self._listener_mapping: dict[
            tuple[Listener[Any], str], _InnerListener
        ] = {}  # used for mapping our listener functions to asyncpg's listener functions

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

    async def add_listener(self, channel: str, payload_type: type[T], callback: Listener[T]) -> None:

        if iscoroutinefunction(callback):

            async def async_listener(
                con_ref: AsyncpgConnection | PoolConnectionProxy, pid: int, channel: str, payload: object, /
            ) -> None:
                parsed_payload = TypeParser.parse(payload_type, payload)

                await callback(Connection(con_ref, self._query_assembler), pid, channel, parsed_payload)

            await self._connection.add_listener(channel, async_listener)
            self._listener_mapping[(callback, channel)] = async_listener

        else:

            def sync_listener(
                con_ref: AsyncpgConnection | PoolConnectionProxy, pid: int, channel: str, payload: object, /
            ) -> None:
                parsed_payload = TypeParser.parse(payload_type, payload)
                callback(Connection(con_ref, self._query_assembler), pid, channel, parsed_payload)

            await self._connection.add_listener(channel, sync_listener)
            self._listener_mapping[(callback, channel)] = sync_listener

    async def remove_listener(self, channel: str, callback: Listener[T]) -> None:
        inner_listener = self._listener_mapping.pop((callback, channel))
        await self._connection.remove_listener(channel, inner_listener)

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
            if self._transaction.status == TransactionStatus.MARKED_FOR_ROLLBACK:
                await self._transaction.rollback()
            else:
                await self._transaction.commit()
        else:
            await self._transaction.rollback()
            raise exc_val

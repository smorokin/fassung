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
    """Lifecycle states of a transaction."""

    STARTED = "started"
    COMMITTED = "committed"
    MARKED_FOR_ROLLBACK = "marked_for_rollback"
    ROLLED_BACK = "rolled_back"


class Transaction:
    """Wraps an active database transaction.

    A [fassung.connection.Transaction][] proxies query methods to its parent [fassung.connection.Connection][] while
    guarding against use after commit, rollback, or being marked for rollback.
    Use a [fassung.connection.Connection][] as an async context manager to obtain one.
    """

    def __init__(self, connection: Connection, transaction: AsyncpgTransaction) -> None:
        self._connection: Connection = connection
        self._transaction: AsyncpgTransaction = transaction
        self.status: TransactionStatus = TransactionStatus.STARTED

    async def execute(self, query: Template) -> str:
        """Execute a SQL command and return its status string.

        Args:
            query: A t-string template containing the SQL to execute.
        """
        self._check_status()
        return await self._connection.execute(query)

    def cursor(
        self, type_: type[T], query: Template, *, prefetch: int | None = None, timeout: float | None = None
    ) -> CursorFactory[T]:
        """Create a cursor factory for iterating over query results.

        Args:
            type_: The row type to parse each result into.
            query: A t-string template containing the SQL query.
            prefetch: Number of rows to prefetch.
            timeout: Optional query timeout in seconds.
        """
        self._check_status()
        return self._connection.cursor(type_=type_, query=query, prefetch=prefetch, timeout=timeout)

    async def fetch(self, type_: type[T], query: Template, *, timeout: float | None = None) -> list[T]:
        """Execute a query and return all resulting rows as a typed list.

        Args:
            type_: The row type to parse each result into.
            query: A t-string template containing the SQL query.
            timeout: Optional query timeout in seconds.
        """
        self._check_status()
        return await self._connection.fetch(type_=type_, query=query, timeout=timeout)

    async def fetchval(self, type_: type[T], query: Template, column: int = 0, *, timeout: float | None = None) -> T:
        """Execute a query and return a single scalar value.

        Args:
            type_: The expected type of the returned value.
            query: A t-string template containing the SQL query.
            column: Zero-based column index to extract.
            timeout: Optional query timeout in seconds.
        """
        self._check_status()
        return await self._connection.fetchval(type_=type_, query=query, column=column, timeout=timeout)

    async def fetchrow(self, type_: type[T], query: Template, *, timeout: float | None = None) -> T | None:
        """Execute a query and return the first row, or ``None`` if empty.

        Args:
            type_: The row type to parse the result into.
            query: A t-string template containing the SQL query.
            timeout: Optional query timeout in seconds.
        """
        self._check_status()
        return await self._connection.fetchrow(type_=type_, query=query, timeout=timeout)

    async def rollback(self) -> None:
        """Roll back the transaction."""
        await self._transaction.rollback()
        self.status = TransactionStatus.ROLLED_BACK

    async def commit(self) -> None:
        """Commit the transaction."""
        await self._transaction.commit()
        self.status = TransactionStatus.COMMITTED

    def mark_for_rollback(self) -> None:
        """Mark the transaction for rollback.

        The transaction cannot be used after this call. The actual rollback
        is performed when the [fassung.connection.Connection][] context manager exits.
        """
        self.status = TransactionStatus.MARKED_FOR_ROLLBACK

    def _check_status(self) -> None:
        if self.status != TransactionStatus.STARTED:
            raise TransactionClosedError(f"Transaction is not in started status: {self.status}")


class Connection(AbstractAsyncContextManager[Transaction]):
    """Async database connection that assembles t-string queries and parses results.

    Use as an async context manager to start a [fassung.connection.Transaction][]:

        async with connection as txn:
            await txn.execute(t"...")
    """

    def __init__(self, connection: AsyncpgConnection | PoolConnectionProxy, query_assembler: QueryAssembler) -> None:
        self._connection: AsyncpgConnection | PoolConnectionProxy = connection
        self._query_assembler: QueryAssembler = query_assembler
        self._transaction: Transaction | None = None
        self._listener_mapping: dict[
            tuple[Listener[Any], str], _InnerListener
        ] = {}  # used for mapping our listener functions to asyncpg's listener functions

    async def execute(self, query: Template, *, timeout: float | None = None) -> str:
        """Execute a SQL command and return its status string.

        Args:
            query: A t-string template containing the SQL to execute.
            timeout: Optional query timeout in seconds.
        """
        assembled = self._query_assembler.assemble(query)
        return await self._connection.execute(assembled.query, *assembled.args, timeout=timeout)

    def cursor(
        self, type_: type[T], query: Template, *, prefetch: int | None = None, timeout: float | None = None
    ) -> CursorFactory[T]:
        """Create a cursor factory for iterating over query results.

        Args:
            type_: The row type to parse each result into.
            query: A t-string template containing the SQL query.
            prefetch: Number of rows to prefetch.
            timeout: Optional query timeout in seconds.
        """
        assembled = self._query_assembler.assemble(query)
        return CursorFactory(
            cursor_factory=self._connection.cursor(
                assembled.query, *assembled.args, prefetch=prefetch, timeout=timeout
            ),
            type_=type_,
        )

    async def fetch(self, type_: type[T], query: Template, *, timeout: float | None = None) -> list[T]:
        """Execute a query and return all resulting rows as a typed list.

        Args:
            type_: The row type to parse each result into.
            query: A t-string template containing the SQL query.
            timeout: Optional query timeout in seconds.
        """
        assembled = self._query_assembler.assemble(query)
        raw_list = await self._connection.fetch(assembled.query, *assembled.args, timeout=timeout)
        return TypeParser.parse(list[type_], raw_list)

    async def fetchval(self, type_: type[T], query: Template, column: int = 0, *, timeout: float | None = None) -> T:
        """Execute a query and return a single scalar value.

        Args:
            type_: The expected type of the returned value.
            query: A t-string template containing the SQL query.
            column: Zero-based column index to extract.
            timeout: Optional query timeout in seconds.
        """
        assembled = self._query_assembler.assemble(query)
        raw_value = await self._connection.fetchval(assembled.query, *assembled.args, column=column, timeout=timeout)
        return TypeParser.parse(type_, raw_value)

    async def fetchrow(self, type_: type[T], query: Template, *, timeout: float | None = None) -> T | None:
        """Execute a query and return the first row, or ``None`` if empty.

        Args:
            type_: The row type to parse the result into.
            query: A t-string template containing the SQL query.
            timeout: Optional query timeout in seconds.
        """
        assembled = self._query_assembler.assemble(query)
        raw_row = await self._connection.fetchrow(assembled.query, *assembled.args, timeout=timeout)
        if raw_row is None:
            return None
        return TypeParser.parse(type_, raw_row)

    async def add_listener(self, channel: str, payload_type: type[T], callback: Listener[T]) -> None:
        """Subscribe to PostgreSQL LISTEN/NOTIFY notifications on *channel*.

        The raw notification payload is parsed into *payload_type* before being
        passed to *callback*.

        Args:
            channel: The notification channel name.
            payload_type: The type to parse the payload into.
            callback: The listener to invoke on each notification.
        """

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
        """Unsubscribe a previously registered listener from *channel*.

        Args:
            channel: The notification channel name.
            callback: The listener to remove.
        """
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

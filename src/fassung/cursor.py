from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Generator
from typing import Self, override

from asyncpg.cursor import (
    Cursor as AsyncpgCursor,
    CursorFactory as AsyncpgCursorFactory,
    CursorIterator as AsyncpgCursorIterator,
)

from fassung.type_parser import TypeParser


class CursorIterator[T](AsyncIterator[T]):
    """Async iterator that yields typed rows from a query result.

    Not intended to be created directly — use [fassung.cursor.CursorFactory][] in an
    ``async for`` loop instead.
    """

    def __init__(self, cursor: AsyncpgCursorIterator, type_: type[T]) -> None:
        self._iterator: AsyncpgCursorIterator = cursor
        self._type: type[T] = type_

    @override
    def __aiter__(self) -> Self:
        return self

    @override
    async def __anext__(self) -> T:
        raw_record = await self._iterator.__anext__()
        return TypeParser.parse(self._type, raw_record)


class Cursor[T]:
    """Interactive database cursor for fetching typed rows on demand.

    Not intended to be created directly — await a [fassung.cursor.CursorFactory][] instead.
    """

    def __init__(self, cursor: AsyncpgCursor, type_: type[T]) -> None:
        self._cursor: AsyncpgCursor = cursor
        self._type: type[T] = type_

    async def fetch(self, n: int, *, timeout: float | None = None) -> list[T]:
        """Fetch the next *n* rows and return them as a typed list.

        Args:
            n: Maximum number of rows to fetch.
            timeout: Optional query timeout in seconds.
        """
        raw_list = await self._cursor.fetch(n, timeout=timeout)
        return TypeParser.parse(list[self._type], raw_list)

    async def fetchrow(self, *, timeout: float | None = None) -> T | None:
        """Fetch the next row, or return ``None`` if exhausted.

        Args:
            timeout: Optional query timeout in seconds.
        """
        raw_record = await self._cursor.fetchrow(timeout=timeout)
        if not raw_record:
            return None
        return TypeParser.parse(self._type, raw_record)

    async def forward(self, n: int, *, timeout: float | None = None) -> int:
        """Skip forward *n* rows and return the number of rows actually skipped.

        Args:
            n: Number of rows to skip.
            timeout: Optional query timeout in seconds.
        """
        return await self._cursor.forward(n, timeout=timeout)


class CursorFactory[T](Awaitable[Cursor[T]], AsyncIterable[T]):
    """Factory that produces a [fassung.cursor.Cursor][] when awaited or a
    [fassung.cursor.CursorIterator][] when iterated.

    Obtain an instance via [fassung.connection.Connection.cursor][] or
    [fassung.connection.Transaction.cursor][].
    """

    def __init__(self, cursor_factory: AsyncpgCursorFactory, type_: type[T]) -> None:
        self._cursor_factory: AsyncpgCursorFactory = cursor_factory
        self._type: type[T] = type_

    @override
    def __aiter__(self) -> CursorIterator[T]:
        iterator = self._cursor_factory.__aiter__()
        return CursorIterator(iterator, self._type)

    @override
    def __await__(self) -> Generator[None, None, Cursor[T]]:
        cursor = yield from self._cursor_factory.__await__()
        return Cursor(cursor, self._type)

from collections.abc import Awaitable
from typing import TYPE_CHECKING, Protocol, TypeVar

if TYPE_CHECKING:
    from fassung.connection import Connection


T = TypeVar("T")


class Listener[T](Protocol):
    def __call__(
        self,
        con_ref: Connection,
        pid: int,
        channel: str,
        payload: T,
        /,
    ) -> Awaitable[None] | None: ...

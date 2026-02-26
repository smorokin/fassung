from collections.abc import Awaitable
from typing import TYPE_CHECKING, Protocol, TypeVar

if TYPE_CHECKING:
    from fassung.connection import Connection


T = TypeVar("T")


class Listener[T](Protocol):
    """Protocol for LISTEN/NOTIFY callbacks.

    A listener receives the connection that triggered the notification, the
    backend process ID, the channel name, and the parsed payload. It may be
    synchronous or asynchronous.
    """

    def __call__(
        self,
        con_ref: Connection,
        pid: int,
        channel: str,
        payload: T,
        /,
    ) -> Awaitable[None] | None: ...

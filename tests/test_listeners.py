import asyncio

import pytest

from fassung import Connection


async def test_async_listener(connection: Connection) -> None:
    """Test that an async listener receives NOTIFY payloads and parses them correctly."""
    received: list[tuple[int, str, str]] = []
    event = asyncio.Event()

    async def on_notify(con: Connection, pid: int, channel: str, payload: str) -> None:
        received.append((pid, channel, payload))
        event.set()

    await connection.add_listener("test_channel", str, on_notify)
    try:
        await connection.execute(t"NOTIFY test_channel, 'hello'")
        await asyncio.wait_for(event.wait(), timeout=5.0)

        assert len(received) == 1
        _, ch, pl = received[0]
        assert ch == "test_channel"
        assert pl == "hello"
    finally:
        await connection.remove_listener("test_channel", on_notify)


async def test_sync_listener(connection: Connection) -> None:
    """Test that a synchronous (non-async) listener also works."""
    received: list[str] = []

    def on_notify_sync(con: Connection, pid: int, channel: str, payload: str) -> None:
        received.append(payload)

    await connection.add_listener("sync_ch", str, on_notify_sync)
    try:
        await connection.execute(t"NOTIFY sync_ch, 'sync_msg'")
        # Sync callbacks are invoked inline, but give a small window for delivery
        await asyncio.sleep(0.5)

        assert received == ["sync_msg"]
    finally:
        await connection.remove_listener("sync_ch", on_notify_sync)


async def test_payload_type_parsing(connection: Connection) -> None:
    """Test that the payload is parsed to the specified type."""
    received_values: list[int] = []
    event = asyncio.Event()

    async def on_notify(con: Connection, pid: int, channel: str, payload: int) -> None:
        received_values.append(payload)
        event.set()

    await connection.add_listener("int_channel", int, on_notify)
    try:
        await connection.execute(t"NOTIFY int_channel, '42'")
        await asyncio.wait_for(event.wait(), timeout=5.0)

        assert len(received_values) == 1
        assert received_values[0] == 42
        assert isinstance(received_values[0], int)
    finally:
        await connection.remove_listener("int_channel", on_notify)


async def test_remove_listener(connection: Connection) -> None:
    """Test that a removed listener no longer receives notifications."""
    call_count = 0

    async def on_notify(con: Connection, pid: int, channel: str, payload: str) -> None:
        nonlocal call_count
        call_count += 1

    await connection.add_listener("remove_test", str, on_notify)
    await connection.remove_listener("remove_test", on_notify)

    await connection.execute(t"NOTIFY remove_test, 'should_not_arrive'")
    await asyncio.sleep(0.5)

    assert call_count == 0


async def test_remove_unregistered_listener_raises(connection: Connection) -> None:
    """Test that removing a listener that was never added raises KeyError."""

    async def on_notify(con: Connection, pid: int, channel: str, payload: str) -> None:
        pass  # pragma: no cover

    with pytest.raises(KeyError):
        await connection.remove_listener("nonexistent", on_notify)


async def test_multiple_listeners_on_same_channel(connection: Connection) -> None:
    """Test that multiple listeners on the same channel all receive notifications."""
    results_a: list[str] = []
    results_b: list[str] = []
    event_a = asyncio.Event()
    event_b = asyncio.Event()

    async def listener_a(con: Connection, pid: int, channel: str, payload: str) -> None:
        results_a.append(payload)
        event_a.set()

    async def listener_b(con: Connection, pid: int, channel: str, payload: str) -> None:
        results_b.append(payload)
        event_b.set()

    await connection.add_listener("multi_ch", str, listener_a)
    await connection.add_listener("multi_ch", str, listener_b)
    try:
        await connection.execute(t"NOTIFY multi_ch, 'broadcast'")
        await asyncio.wait_for(asyncio.gather(event_a.wait(), event_b.wait()), timeout=5.0)

        assert results_a == ["broadcast"]
        assert results_b == ["broadcast"]
    finally:
        await connection.remove_listener("multi_ch", listener_a)
        await connection.remove_listener("multi_ch", listener_b)

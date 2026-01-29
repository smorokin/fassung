from asyncpg import create_pool

from fassung.pool import Connection, Pool
from fassung.query_assembler import QueryAssembler


async def test_pool(connection_string: str) -> None:
    async with create_pool(connection_string) as asyncpg_pool:
        pool = Pool(asyncpg_pool, QueryAssembler())
        async with pool.acquire() as connection:
            assert connection
            assert isinstance(connection, Connection)

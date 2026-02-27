# Usage

The main entry point of the library is the [fassung.pool.Pool][] class. It should be created at the entry point of your application and passed around to where you need it.

In simple scripts you can just create it in your main function. It is recommended to use environment variables for the connection string since it contains secrets. In our case we assume an environment variable `DATABASE_URL="postgresql://user:password@host:port/database"` is set:


```python
from fassung import Pool
from os import environ

async def main():
    pool = await Pool.from_connection_string(
        environ["DATABASE_URL"]
    )
    ...
```


In larger applications you might want to use a dependency injection framework to manage its lifecycle. For example with FastAPI:

```python
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from os import environ

from fastapi import FastAPI, Request
from fassung import Pool


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    # Use a lifespan function to create the pool at startup of the fastapi app and add it to the app state
    app.state.pool = await Pool.from_connection_string(
        environ["DATABASE_URL"]
    )
    yield

app = FastAPI(lifespan=lifespan)


async def get_connection(request: Request) -> AsyncGenerator[Connection]:
    # dependency injection function to get a connection from the pool and
    # release it after the request is done
    async with request.app.state.pool.acquire() as connection:
        yield connection


@app.get("/test-db-connection")
async def test_database_connection(connection: Annotated[Connection, Depends(get_connection)]) -> bool:
    # fastapi injects the connection into the endpoint function when
    # the endpoint is called and releases it after the response is sent.
    result = await connection.fetchval(int, t"SELECT 1")
    return result == 1
```

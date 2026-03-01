# Fassung
[![CI](https://github.com/smorokin/fassung/actions/workflows/ci.yml/badge.svg)](https://github.com/smorokin/fassung/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/smorokin/fassung/branch/main/graph/badge.svg)](https://codecov.io/gh/smorokin/fassung)


Fassung combines asyncpg, pydantic and the template strings to provide an ergonomic, type- and SQL-injection-safe interface for working with PostgreSQL databases. It also allows safe nested query composition.

Fassung is very similar to asyncpg. The main difference is that fassung does neither accept positional parameters nor normal python string (`""` or `f""`) as arguments for it's query methods.
Instead it relies on python's new template string literals (`t""`) for query construction. This has several benefits:
- It is SQL-injection-safe: you can't pass a string with SQL code as a parameter
- It is ergonomic: you can pass variables directly into the query, your IDE will provide autocompletion and type checking
- It is composable: you can compose queries by concatenating template strings

Additionally, fassung also uses pydantic to map the query results to python objects, like dataclasses or pydantic models.

It is currently just a proof of concept and not ready for production use. Some features like `COPY` are not implemented yet.

## Documentation

The documentation can be found at [https://smorokin.github.io/fassung/](https://smorokin.github.io/fassung/).

## Installation

Fassung requires python 3.14 because it relies on [template string literals (PEP 750)](https://peps.python.org/pep-0750/). You can install it with

```bash
pip install fassung
```

or

```bash
uv add fassung
```

## Usage

The central class is the `Pool` class, which is a context manager for a connection pool.
You can use the `Pool` class to create connections. Each connection can execute queries and create transactions.

Basic example:

```python
import asyncio
from datetime import date
from string.templatelib import Template
from pydantic import BaseModel

from fassung import Pool, Transaction


class Student(BaseModel):
    id: int
    name: str
    birth_date: date


async def fetch_all(
    transaction: Transaction, limit: int | None = None, offset: int | None = None, where_clause: Template = t""
) -> tuple[int, list[Student]]:

    limit_query = t""
    if limit is not None:
        limit_query = t"LIMIT {limit}"

    offset_query = t""
    if offset is not None:
        offset_query = t"OFFSET {offset}"

    count_query = t"SELECT COUNT(*) FROM students {where_clause}"
    count = await transaction.fetchval(int, count_query)

    query = t"SELECT * FROM students {limit_query} {offset_query} {where_clause}"
    students = await transaction.fetch(Student, query)

    return count, students


async def main():
    pool = Pool.from_connection_string("postgresql://user:password@localhost:5432/testdb")
    async with pool.acquire() as connection:
        async with connection as transaction:
            age = 18
            students: list[Student] = await transaction.fetch(
                Student, t"SELECT * FROM students WHERE age = {age}"
            )
            for student in students:
                print(student.name)

            since = date(2002, 5, 14)
            count, students = await fetch_all(transaction, where_clause=t"WHERE birth_date = {since}")

if __name__ == "__main__":
    asyncio.run(main())
```

More examples can be found in the `tests/examples` directory.


## Contributing

We use uv for dependency management, ruff for formatting & linting, pyright for type checking, and pytest for testing. For information on how to set up the development environment and contribute to `fassung`, please see the [Contributing Guide](CONTRIBUTING.md).


## License

Fassung is licensed under the MIT license.


## Why is it called fassung?
Fassung is the german word for "frame" or "socket". Since it assembles several great python libraries and features (asyncpg, pydantic and template strings) into a single package, it seemed appropriate.


## Influences
The idea for the integration of pydantic with asyncpg came from the [onlymaps](https://github.com/manoss96/onlymaps) project.

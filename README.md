# Fassung

Fassung combines asyncpg, pydantic and the template strings to provide an ergonomic, type- and SQL-injection-safe interface for working with PostgreSQL databases.

It is currently just a proof of concept and not ready for production use.

## Installation

Fassung requires python 3.14. You can install it with

```bash
pip install fassung
```

or 

```bash
uv add fassung
```

## Usage

Fassung is very similar to asyncpg. The central class is the `Pool` class, which is a context manager for a connection pool. 
You can use the `Pool` class as a context manager to create connections. Each connection can execute queries.

```python
from fassung import Pool
from pydantic import BaseModel


class Student(BaseModel):
    id: int
    name: str
    age: int


async def main():
    pool = Pool()
    async with pool.acquire() as connection:
        async with connection as transaction:
            age = 18
            students: list[Student] = await transaction.fetch(
                t"SELECT * FROM students WHERE age = {age}", Student
            )
            for student in students:
                print(student.name)
```
from datetime import UTC, date, datetime
from string.templatelib import Template

import pytest

from fassung import Transaction
from tests.models import Student


class StudentRepository:
    def __init__(self, transaction: Transaction) -> None:
        self._transaction: Transaction = transaction

    async def create(self, student: Student) -> None:
        query = t"""INSERT INTO students (
            id,
            full_name,
            email,
            birth_date,
            major,
            gpa,
            is_active,
            enrolled_at,
            last_seen_at
        ) VALUES (
            {student.id},
            {student.full_name},
            {student.email},
            {student.birth_date},
            {student.major},
            {student.gpa},
            {student.is_active},
            {student.enrolled_at},
            {student.last_seen_at}
        )"""
        _ = await self._transaction.execute(query)

    async def update(self, student: Student) -> None:
        query = t"""UPDATE students
        SET
            full_name = {student.full_name},
            email = {student.email},
            birth_date = {student.birth_date},
            major = {student.major},
            gpa = {student.gpa},
            is_active = {student.is_active},
            enrolled_at = {student.enrolled_at},
            last_seen_at = {student.last_seen_at}
        WHERE
            id = {student.id}
        """
        _ = await self._transaction.execute(query)

    async def delete(self, student_id: int) -> None:
        query = t"""DELETE FROM students WHERE id = {student_id}"""
        _ = await self._transaction.execute(query)

    async def fetch(self, student_id: int) -> Student | None:
        query = t"""SELECT * FROM students WHERE id = {student_id}"""
        return await self._transaction.fetchrow(Student, query)

    async def fetch_all(
        self, limit: int | None = None, offset: int | None = None, where_clause: Template = t""
    ) -> tuple[int, list[Student]]:

        limit_query = t""
        if limit is not None:
            limit_query = t"LIMIT {limit}"

        offset_query = t""
        if offset is not None:
            offset_query = t"OFFSET {offset}"

        count_query = t"SELECT COUNT(*) FROM students {where_clause}"
        count = await self._transaction.fetchval(int, count_query)

        query = t"SELECT * FROM students {limit_query} {offset_query} {where_clause}"
        students = await self._transaction.fetch(Student, query)

        return count, students


@pytest.fixture
def crud(transaction: Transaction) -> StudentRepository:
    return StudentRepository(transaction)


async def test_crud_create(crud: StudentRepository) -> None:
    student = Student(
        id=3,
        full_name="Jack Doe",
        email="jack@example.com",
        birth_date=date(2002, 5, 14),
        major="Physics",
        gpa=3.9,
        is_active=True,
        enrolled_at=datetime(2023, 9, 1, 9, 0, 0, tzinfo=UTC),
        last_seen_at=datetime(2024, 2, 1, 10, 30, 0, tzinfo=UTC),
    )
    await crud.create(student)


async def test_crud_update(crud: StudentRepository) -> None:
    student = Student(
        id=3,
        full_name="Jack Doe",
        email="jack@example.com",
        birth_date=date(2002, 5, 14),
        major="Physics",
        gpa=3.9,
        is_active=True,
        enrolled_at=datetime(2023, 9, 1, 9, 0, 0, tzinfo=UTC),
        last_seen_at=datetime(2024, 2, 1, 10, 30, 0, tzinfo=UTC),
    )
    await crud.update(student)


async def test_crud_delete(crud: StudentRepository) -> None:
    await crud.delete(2)


async def test_crud_fetch(crud: StudentRepository) -> None:
    student = await crud.fetch(1)
    assert student is not None
    assert student.id == 1


async def test_crud_fetch_all(crud: StudentRepository) -> None:
    count, students = await crud.fetch_all()
    assert count == 2
    assert len(students) == 2


async def test_crud_fetch_all_with_limit(crud: StudentRepository) -> None:
    count, students = await crud.fetch_all(limit=1)
    assert count == 2
    assert len(students) == 1


async def test_crud_fetch_all_with_offset(crud: StudentRepository) -> None:
    count, students = await crud.fetch_all(offset=1)
    assert count == 2
    assert len(students) == 1


async def test_crud_fetch_all_with_limit_and_offset(crud: StudentRepository) -> None:
    count, students = await crud.fetch_all(limit=1, offset=1)
    assert count == 2
    assert len(students) == 1


async def test_crud_fetch_all_with_where_clause(crud: StudentRepository) -> None:
    count, students = await crud.fetch_all(where_clause=t"WHERE id = 1")
    assert count == 1
    assert len(students) == 1


async def test_crud_fetch_all_with_where_clause_date(crud: StudentRepository) -> None:
    since = date(2002, 5, 14)
    count, students = await crud.fetch_all(where_clause=t"WHERE birth_date = {since}")
    assert count == 1
    assert len(students) == 1

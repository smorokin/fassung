from datetime import date

from pydantic import AwareDatetime, BaseModel


class Student(BaseModel):
    id: int
    full_name: str
    email: str
    birth_date: date
    major: str
    gpa: float
    is_active: bool
    enrolled_at: AwareDatetime
    last_seen_at: AwareDatetime | None = None

from pydantic import BaseModel


class Student(BaseModel):
    id: int
    name: str
    field_int: int
    field_bool: bool
    field_float: float
    field_str: str

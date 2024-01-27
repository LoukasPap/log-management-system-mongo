from pydantic import BaseModel


class Admin(BaseModel):
    name: str
    age: int

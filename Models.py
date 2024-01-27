from datetime import datetime

import pydantic
from pydantic import BaseModel


class Admin(BaseModel):
    username: str
    age: int

    # email: str
    # telephone: str
    # votes_count: int
    # voted_logs: list
    # voted_ips: set


class Date(BaseModel):
    date: datetime

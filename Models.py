from datetime import datetime

import pydantic
from pydantic import BaseModel, Field


class Admin(BaseModel):
    username: str
    age: int
    email: str
    telephone: str
    votes_count: int
    voted_logs: list
    voted_ips: set


class Date(BaseModel):
    date: datetime
    AccessLogs: list
    ReplicateLogs: list
    ServeLogs: list
    ReceiveLogs: list


class AccessLog(BaseModel):
    # General log fields
    logID: str
    date: datetime
    ip: str
    log_type: str = Field(default="Access")

    # Access specific fields
    http_method: str
    http_response_status: str
    http_response_size: int
    remote_name: str
    user_agent_string: str

    # Admin fields
    votes: int
    voted_by: list


class ReplicateLog(BaseModel):
    logID: str


class ServeLog(BaseModel):
    logID: str


class ReceiveLog(BaseModel):
    logID: str


from datetime import datetime

import pydantic
from pydantic import BaseModel, Field


class Admin(BaseModel):
    username: str
    email: str
    telephone: str
    votes_count: int
    voted_logs: list
    voted_ips: set


class AccessLog(BaseModel):
    # General log fields
    log_id: str
    timestamp: datetime
    ip: str
    log_type: str = Field(default="access")

    # Access specific fields
    http_method: str
    http_response_status: str
    http_response_size: int
    remote_name: str
    user_agent_string: str
    referer: str
    resource: str

    # Admin fields
    votes: int
    voted_by: list


class ReplicateLog(BaseModel):
    log_id: str


class ServeLog(BaseModel):
    log_id: str


class ReceivedLog(BaseModel):
    log_id: str


class ReceivingLog(BaseModel):
    log_id: str


class Date(BaseModel):
    _id: datetime

    access_logs: list[AccessLog]
    access_log_count: int

    replicate_logs: list[ReplicateLog]
    replicate_log_count: int

    serve_logs: list[ServeLog]
    serve_log_count: int

    received_logs: list[ReceivedLog]
    received_log_count: int

    receiving_logs: list[ReceivingLog]
    receiving_log_count: int

from datetime import datetime
from typing import Union

from pydantic import BaseModel, Field


class Admin(BaseModel):
    username: str
    email: str
    telephone: str
    votes_count: int
    voted_logs: list
    voted_ips: dict


class AccessLog(BaseModel):
    # General log fields
    timestamp: datetime
    ip: str
    log_type: str = Field(default="access")

    # Access specific fields
    http_method: str
    http_response_status: str
    http_response_size: Union[str, None]
    user_id: Union[str, None]
    remote_name: Union[str, None]
    user_agent_string: Union[str, None]
    referer: Union[str, None]
    resource: Union[str, None]

    # Admin fields
    votes: int
    voted_by: Union[list[Admin], None]


class ReplicateLog(BaseModel):
    # General log fields
    timestamp: datetime
    ip: str
    log_type: str = Field(default="replicate")

    # Replicate specific fields
    block_ids: list
    size: int
    destination_ip: str

    # Admin fields
    votes: int
    voted_by: list


class ServedLog(BaseModel):
    # General log fields
    timestamp: datetime
    ip: str
    log_type: str = Field(default="served")

    # Replicate specific fields
    block_ids: list
    size: int
    destination_ip: str

    # Admin fields
    votes: int
    voted_by: list


class ReceivedLog(BaseModel):
    # General log fields
    timestamp: datetime
    ip: str
    log_type: str = Field(default="received")

    # Replicate specific fields
    block_ids: list
    size: int
    destination_ip: str

    # Admin fields
    votes: int
    voted_by: list


class ReceivingLog(BaseModel):
    # General log fields
    timestamp: datetime
    ip: str
    log_type: str = Field(default="receiving")

    # Replicate specific fields
    block_ids: list
    size: int
    destination_ip: str

    # Admin fields
    votes: int
    voted_by: list


class Date(BaseModel):
    _id: datetime

    access_logs: list[AccessLog]
    access_log_count: int

    replicate_logs: list[ReplicateLog]
    replicate_log_count: int

    served_logs: list[ServedLog]
    served_log_count: int

    received_logs: list[ReceivedLog]
    received_log_count: int

    receiving_logs: list[ReceivingLog]
    receiving_log_count: int


class Referer(BaseModel):
    referer: Union[str, None]
    resources: dict

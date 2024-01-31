from datetime import datetime
from typing import Union
from pydantic import BaseModel, Field


class Admin(BaseModel):
    username: str
    email: str
    telephone: str
    votes_count: int = Field(default=0)
    voted_logs: list = Field(default=[])
    voted_ips: list = Field(default=[])


class AccessLog(BaseModel):
    # General fields
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


class HadoopFSLog(BaseModel):
    # General fields
    timestamp: datetime
    ip: str
    log_type: str

    # HDFS specific fields
    block_ids: list[str]
    destination_ip: list[str]
    size: int = Field(default=0)

    # Admin fields
    votes: int
    voted_by: list


class Referer(BaseModel):
    referer: Union[str, None]
    resources: list

from datetime import datetime
from datetime import timedelta
from models import Referer

from bson import json_util
import json

# PyMongo Client
from pymongo import MongoClient


def parse_json(data):
    return json.loads(json_util.dumps(data))


def return_some_results(cursor):
    temp = []
    for i in range(100):
        doc = cursor.try_next()
        if not doc:
            break
        temp.append(doc)
    return temp


client = MongoClient("localhost", 27017)
db = client["NoSQL-LOGS"]
access_logs = db["access_logs"]
hdfs_logs = db["hdfs_logs"]
admins = db["admins"]
referrers = db["referers"]


async def query1(date_start, date_end):
    pipeline = [
        {
            "$unionWith": {
                "coll": "hdfs_logs"
            }
        },
        {
            "$match": {
                "timestamp": {"$gte": date_start, "$lte": date_end}
            }
        },
        {
            "$group": {
                "_id": "$log_type",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        }
    ]
    cursor = access_logs.aggregate(pipeline)
    return parse_json(cursor)


async def query2(date_start, date_end, log_type):
    if log_type == "access":
        collection = access_logs
    else:
        collection = hdfs_logs

    pipeline = [
        {
            "$match": {
                "timestamp": {"$gte": date_start, "$lte": date_end}
            }
        },
        {
            "$match": {
                "log_type": log_type
            }
        },
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$timestamp"},
                    "month": {"$month": "$timestamp"},
                    "day": {"$dayOfMonth": "$timestamp"}
                },
                "count": {"$sum": 1}
            }
        }
    ]
    cursor = collection.aggregate(pipeline)
    return parse_json(cursor)


async def query3(date):
    date_start = datetime(year=date.year, month=date.month, day=date.day)
    date_end = date_start + timedelta(days=1)

    pipeline = [
        {
            "$unionWith": {
                "coll": "hdfs_logs"
            }
        },
        {
            "$match": {
                "timestamp": {"$gte": date_start, "$lt": date_end}
            }
        },
        {
            "$group": {
                "_id": {
                    "source_ip": "$ip",
                    "log_type": "$log_type"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        },
        {
            "$group": {
                "_id": "$_id.source_ip",
                "top_logs": {
                    "$push": {
                        "log_type": "$_id.log_type",
                        "count": "$count"
                    }
                },
                "total": {"$sum": "$count"}
            }
        },
        {
            "$sort": {"total": -1}
        },
        {
            "$project": {
                "_id": 0,
                "source_ip": "$_id",
                "top_logs": {"$slice": ["$top_logs", 3]},
                "total": 1
            }
        }
    ]
    cursor = access_logs.aggregate(pipeline)

    print("query 3 aggregate done!")
    temp = return_some_results(cursor)
    return parse_json(temp)


async def query4(date_start, date_end):
    pipeline = [
        {
            "$match": {
                "timestamp": {"$gte": date_start, "$lte": date_end}
            }
        },
        {
            "$group": {
                "_id": "$http_method",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": 1}
        }
    ]
    cursor = access_logs.aggregate(pipeline)
    top2 = []
    for i in range(2):
        temp = cursor.try_next()
        if not temp:
            break
        top2.append(temp)
    return parse_json(top2)


async def query5():
    pipeline = [
        {
            "$project": {
                "referer": 1,
                "resourcesNumber": {"$size": "$resources"}
            }
        },
        {
            "$match": {
                "$and": [
                    {"referer": {"$ne": None}},
                    {"resourcesNumber": {"$gte": 2}}
                ]
            }
        }
    ]
    cursor = referrers.aggregate(pipeline)
    return parse_json(cursor)


async def query6():
    pipeline = [
        {
            "$match": {
                "log_type": {
                    "$in": ["Replicate", "Served"]
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "block_ids": "$block_ids",
                    "log_type": "$log_type",
                    "timestamp": "$timestamp"
                }
            }
        },
        {
            "$group": {
                "_id": "$_id.block_ids",
                "data": {
                    "$push": {
                        "log_type": "$_id.log_type",
                        "timestamp": "$_id.timestamp",
                    }
                }
            }
        }
    ]
    cursor = hdfs_logs.aggregate(pipeline)
    top2 = []
    for i in range(10):
        temp = cursor.try_next()
        if not temp:
            break
        top2.append(temp)
    return parse_json(top2)

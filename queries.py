from datetime import datetime
from datetime import timedelta
from models import Referer

from bson import json_util
import json

# PyMongo Client
from pymongo import MongoClient


def parse_json(data):
    return json.loads(json_util.dumps(data))


client = MongoClient("localhost", 27017)
db = client["NoSQL-LOGS"]
access_logs = db["access"]
hdfs_logs = db["hdfs_logs"]
admins = db["admins"]
referrers = db["referrers"]


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
                    "source_ip": "$source_ip",
                    "log_type": "$log_type"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        }
    ]
    cursor = access_logs.aggregate(pipeline)
    top3 = []
    for i in range(2):
        temp = cursor.try_next()
        if not temp:
            break
        top3.append(temp)
    return parse_json(top3)

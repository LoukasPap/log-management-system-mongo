from datetime import datetime
from models import Referer

from bson import json_util
import json


def parse_json(data):
    return json.loads(json_util.dumps(data))

# PyMongo Client
from pymongo import MongoClient

client = MongoClient("localhost", 27017)
db = client["NoSQL-LOGS"]
test1 = db["test1"]
test2 = db["test2"]


async def query1():
    pipeline = [
        {
            "$unionWith": {
                "coll": "test2"
            }
        }
    ]
    cursor = test1.aggregate(pipeline)
    return parse_json(cursor)


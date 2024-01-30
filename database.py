from datetime import datetime
from models import Referer

# Motor Client
import motor.motor_asyncio

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
database = client["NoSQL-LOGS"]
collection_admins = database.admins
collection_access = database["access"]


# PyMongo Client
from pymongo import MongoClient

client2 = MongoClient("localhost", 27017)
db = client2["NoSQL-LOGS"]
collection_dt = db["dates"]
collection_ref = db["referers"]


def insert_a_log(log):
    # print(log)
    # if log["log_type"] == "access":
    #     collection_ref.update_one(
    #         {'referer': log["referer"]},
    #         {
    #             "$push": {log["resource"]}
    #         },
    #         upsert=True
    #     )

    timestamp: datetime = log["timestamp"]
    result = collection_dt.update_one(
        {"_id": datetime(year=timestamp.year,
                         month=timestamp.month,
                         day=timestamp.day,
                         hour=timestamp.hour
                         )},
        {
            "$push": {log["log_type"] + "_logs": log},
            "$inc": {log["log_type"] + "_log_count": 1}
        },
        upsert=True
    )
    return result


async def fetch_one_admin(name):
    document = await collection_admins.find_one({"username": name})
    return document


async def create_admin(admin):
    document = admin
    result = await collection_admins.insert_one(document)
    return document


async def insert_log(log):
    result = await collection_access.update_one({'_id': log["timestamp"]},  # ! wrong timestamp
                                                {'$push': {log["log_type"] + "_logs": log},
                                                 '$inc': {log["log_type"] + "_log_count": 1}},
                                                upsert=True)

    print(result.matched_count, result.modified_count)

from datetime import datetime
from models import Referer

# Motor Client
import motor.motor_asyncio

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
database = client["NoSQL-LOGS"]
collection_admins = database.admins
collection_access = database["access"]



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

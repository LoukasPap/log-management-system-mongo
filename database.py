import motor.motor_asyncio

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
database = client.get_database("NoSQL-LOGS")
collection = database.admins


async def fetch_one_admin(name):
    document = await collection.find_one({"username": name})
    return document


async def create_admin(admin):
    document = admin
    result = await collection.insert_one(document)
    return document


async def insert_log(log):
    result = ""
    if log.log_type == "Access":
        result = await collection.update_one({'date': log.date},
                                             {'$push': {'AccessLogs': log}},
                                             upsert=True)
    elif log.log_type == "Receive":
        result = await collection.update_one({'date': log.date},
                                             {'$push': {'ReceiveLogs': log}},
                                             upsert=True)
    elif log.log_type == "Replicate":
        result = await collection.update_one({'date': log.date},
                                             {'$push': {'ReplicateLogs': log}},
                                             upsert=True)
    elif log.log_type == "Serve":
        result = await collection.update_one({'date': log.date},
                                             {'$push': {'ServeLogs': log}},
                                             upsert=True)

    print('matched %d, modified %d' %
          (result.matched_count, result.modified_count))


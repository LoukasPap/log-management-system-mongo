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
    if log.log_type == "access":
        result = await collection.update_one({'_id': log.date.date()},
                                             {'$push': {'access_logs': log},
                                              '$inc': {'access_log_count': 1}},
                                             upsert=True)
    elif log.log_type == "received":
        result = await collection.update_one({'_id': log.date.date()},
                                             {'$push': {'received_logs': log},
                                              '$inc': {'received_log_count': 1}},
                                             upsert=True)
    elif log.log_type == "receiving":
        result = await collection.update_one({'_id': log.date.date()},
                                             {'$push': {'receiving_logs': log},
                                              '$inc': {'receiving_log_count': 1}},
                                             upsert=True)
    elif log.log_type == "replicate":
        result = await collection.update_one({'_id': log.date.date()},
                                             {'$push': {'replicate_logs': log},
                                              '$inc': {'replicate_log_count': 1}},
                                             upsert=True)
    elif log.log_type == "serve":
        result = await collection.update_one({'_id': log.date.date()},
                                             {'$push': {'serve_logs': log},
                                              '$inc': {'serve_log_count': 1}},
                                             upsert=True)

    print('matched %d, modified %d' %
          (result.matched_count, result.modified_count))


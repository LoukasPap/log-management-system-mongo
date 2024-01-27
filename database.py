import motor.motor_asyncio

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
database = client.get_database("NoSQL-LOGS")
collection = database.admins


async def fetch_one_admin(name):
    document = await collection.find_one({"name": name})
    return document

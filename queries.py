import json
from datetime import timedelta

from bson import json_util
# PyMongo Client
from pymongo import MongoClient

from models import *
from bson import ObjectId


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
            # Combines the collections to get all logs
            "$unionWith": {
                "coll": "hdfs_logs"
            }
        },
        {
            # Gets only the logs inside the required timestamps
            "$match": {
                "timestamp": {"$gte": date_start, "$lte": date_end}
            }
        },
        {
            # Counts the logs per type
            "$group": {
                "_id": "$log_type",
                "count": {"$sum": 1}
            }
        },
        {
            # Sorts in descending order
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
            # Gets only the logs inside the required timestamps
            "$match": {
                "timestamp": {"$gte": date_start, "$lte": date_end}
            }
        },
        {
            # Gets only the logs of the required type
            "$match": {
                "log_type": log_type
            }
        },
        {
            # Groups only by the date portion of the timestamp and counts the logs
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
    # Get the single day time range
    date_start = datetime(year=date.year, month=date.month, day=date.day)
    date_end = date_start + timedelta(days=1)

    pipeline = [
        {
            # Combines the collections to get all logs
            "$unionWith": {
                "coll": "hdfs_logs"
            }
        },
        {
            # Gets only the logs inside the required timestamps
            "$match": {
                "timestamp": {"$gte": date_start, "$lt": date_end}
            }
        },
        {
            # Group by ip and then log type
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
            # Group by ip by adding the counts for each log type
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
            # Keep the top 3 log types per ip
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
            # Gets only the logs inside the required timestamps
            "$match": {
                "timestamp": {"$gte": date_start, "$lte": date_end}
            }
        },
        {
            # Group by http method and count the logs
            "$group": {
                "_id": "$http_method",
                "count": {"$sum": 1}
            }
        },
        {
            # Sort in ascending order
            "$sort": {"count": 1}
        }
    ]
    # Only keep the top 2 results - the less common http types
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
            # Get the referers with the size of their resource array
            "$project": {
                "referer": 1,
                "resourcesNumber": {"$size": "$resources"}
            }
        },
        {
            # Get only the resources that are connected with a referrer
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
            # Get only the replicate and served logs
            "$match": {
                "$or": [
                    {"log_type": "Replicate"},
                    {"log_type": "Served"}
                ]
            }
        },
        {
            "$unwind": "$block_ids"
        },
        {
            # Keep only the relevant information with only the date portion of timestamp
            "$project": {
                "block_ids": 1,
                "log_type": 1,
                "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}}
            }
        },
        {
            # Group by all the relevant fields
            "$group": {
                "_id": {
                    "block_id": "$block_ids",
                    "log_type": "$log_type",
                    "date": "$date"
                }
            }
        },
        {
            # Then group by block_ids and dates only and combining the log types
            "$group": {
                "_id": {
                    "block_id": "$_id.block_id",
                    "date": "$_id.date"
                },
                "log_type": {
                    "$push": {
                        "log_type": "$_id.log_type"
                    }
                },
                "count": {"$sum": 1}
            }
        },
        {
            # And find the block ids that have been replicated and served on the same day
            "$match": {
                "count": {"$gte": 2}
            }
        },
        {
            # Group by block_ids in case they've been served and replicated on more than one days
            "$group": {
                "_id": "$_id.block_id"
            }
        }
    ]
    cursor = hdfs_logs.aggregate(pipeline)
    print("query 6 aggregate done!")
    temp = return_some_results(cursor)
    return parse_json(temp)


async def query7(date):
    date_start = datetime(year=date.year, month=date.month, day=date.day)
    date_end = date_start + timedelta(days=1)

    pipeline = [
        {
            "$unionWith": {
                "coll": "hdfs_logs"
            }
        },
        {
            # Gets only the logs inside the required timestamps
            "$match": {
                "timestamp": {"$gte": date_start, "$lt": date_end}
            }
        },
        {
            # Sort in descending order by the number of votes
            "$sort": {"votes": -1}
        }
    ]
    # Only keep the top50 results
    cursor = access_logs.aggregate(pipeline)
    print("query 7 aggregate done!")
    top50 = []
    for i in range(50):
        temp = cursor.try_next()
        if not temp:
            break
        top50.append(temp)
    return parse_json(top50)


async def query8():
    pipeline = [
        {
            '$project': {
                'username': 1,
                'email': 1,
                'telephone': 1,
                'votes_count': 1,
            }
        },
        {
            # Sort in descending order by the number of votes
            '$sort': {
                'votes_count': -1
            }
        },
        {
            # Only keep the top50 results
            '$limit': 50
        }
    ]
    cursor = admins.aggregate(pipeline)
    print("query 8 aggregate done!")
    temp = return_some_results(cursor)
    return parse_json(temp)


async def query9():
    pipeline = [
        {
            "$project": {
                "username": 1,
                "email": 1,
                "telephone": 1,
                "votes_count": 1,
                # Find how many ips has each admin voted
                "length_of_ips": {
                    "$size": "$voted_ips"
                }
            }
        }, {
            # Sort by them
            "$sort": {
                "length_of_ips": -1
            }
        }, {
            # And keep the top 50
            "$limit": 50
        }]
    cursor = admins.aggregate(pipeline)
    print("query 9 aggregate done!")
    temp = return_some_results(cursor)
    return parse_json(temp)


async def query10():
    pipeline = [
        {
            '$group': {
                '_id': {
                    'email': '$email',
                    'logs': '$voted_logs'
                },
                'uniqueIds': {
                    '$addToSet': '$_id'
                },
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$group': {
                '_id': '$_id.email',
                'voted_logs': {
                    '$push': '$_id.logs'
                },
                'total': {
                    '$sum': '$count'
                }
            }
        }, {
            '$match': {
                'total': {
                    '$gte': 2
                }
            }
        }
    ]
    cursor = admins.aggregate(pipeline)
    print("query 10 aggregate done!")
    temp = return_some_results(cursor)
    return parse_json(temp)


async def query11(username):
    pipeline = [
        {
            "$unwind": "$block_ids"
        },
        {
            # Find if the username that was given has voted each log
            "$project": {
                "block_ids": 1,
                "voted_by_given_name": {
                    "$in": [username, {"$ifNull": ["$voted_by", []]}]
                }
            }
        },
        {
            # and only keep the logs that were indeed voted
            "$match": {
                "voted_by_given_name": True
            }
        }
    ]
    cursor = hdfs_logs.aggregate(pipeline)
    print("query 11 aggregate done!")
    temp = return_some_results(cursor)
    return parse_json(temp)


async def insert_log(log_type, fields):
    date_format_input = "%y%m%d %H%M%S"
    if log_type == "access":
        log_object = AccessLog(
            ip=fields[0],
            remote_name=(None if fields[1] == "-" else fields[1]),
            user_id=(None if fields[2] == "-" else fields[2]),
            timestamp=datetime.strptime(fields[3], date_format_input),
            http_method=fields[4],
            resource=("None" if fields[5] in ("-", " ", "") else fields[5]),
            http_response_status=fields[6],
            http_response_size=(None if fields[7] == "-" else fields[7]),
            referer=(None if fields[-2] == "-" else fields[-2]),
            user_agent_string=(None if fields[-1] == "-" else fields[-1]),
            votes=0,
            voted_by=[]
        )
        response = access_logs.insert_one(dict(log_object))


    else:
        print(fields)
        log_object = HadoopFSLog(
            timestamp=datetime.strptime(fields[0], date_format_input),
            log_type=fields[1],
            block_ids=fields[2].split(),
            ip=fields[3],
            destination_ip=fields[4].split(),
            votes=0,
            voted_by=[]
        )
        if log_type == "dataxceiver":
            log_object.size = int(fields[-1])

        response = hdfs_logs.insert_one(dict(log_object))


    return f"{log_type} log insertion done with id [{response.inserted_id}]!"


async def upvote(username, lid, log_type):
    collection = hdfs_logs
    if log_type == "access":
        collection = access_logs

    doc = collection.find_one_and_update(
        {"_id": ObjectId(lid)},
        {
            "$push": {"voted_by": username},
            "$inc": {"votes": 1}
        }
    )

    admins.find_one_and_update(
        {"username": username},
        {
            "$push": {"voted_logs": lid},
            "$addToSet": {"voted_ips": doc["ip"]},
            "$inc": {"votes_count": 1}
        }
    )

    return f"{username} liked log with id [{lid}]!"

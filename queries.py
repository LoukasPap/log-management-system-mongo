import json
from datetime import timedelta

from bson import json_util
# PyMongo Client
from pymongo import MongoClient

from models import *


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
            "$project": {
                "block_ids": 1,
                "log_type": 1,
                "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}}
            }
        },
        {
            "$group": {
                "_id": {
                    "block_id": "$block_ids",
                    "log_type": "$log_type",
                    "date": "$date"
                }
            }
        },
        {
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
            "$match": {
                "count": {"$gte": 2}
            }
        },
        {
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
            "$match": {
                "timestamp": {"$gte": date_start, "$lt": date_end}
            }
        },
        {
            "$sort": {"votes": -1}
        }
    ]
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
        }, {
            '$sort': {
                'votes_count': -1
            }
        }, {
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
                "length_of_ips": {
                    "$size": "$voted_ips"
                }
            }
        }, {
            "$sort": {
                "length_of_ips": -1
            }
        }, {
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
            "$project": {
                "block_ids": 1,
                "voted_by_given_name": {
                    "$in": [username, {"$ifNull": ["$voted_by", []]}]
                }
            }
        },
        {
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

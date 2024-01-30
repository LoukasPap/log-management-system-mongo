import csv
from models import *

# PyMongo Client
from pymongo import MongoClient

client = MongoClient("localhost", 27017)
db = client["NoSQL-LOGS"]
collection_access = db["access"]
collection_hdfs = db["hdfs"]
collection_ref = db["referers"]

date_format_input = "%y%m%d %H%M%S"
date_format_output = "%y%m%d%H"

access_logs: str = "./logs/access_log_full.csv"
datacxeiver_logs: str = "./logs/hdfs_dataxceiver.csv"
fsnamesystem_logs: str = "./logs/hdfs_fsnamesystem.csv"


def main():
    # insert_access()
    insert_hdfs()


def insert_access():
    log_object: AccessLog

    with open(access_logs, "r", newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader, None)
        for row in reader:
            log_object = AccessLog(
                ip=row[0],
                remote_name=(None if row[1] == '-' else row[1]),
                user_id=(None if row[2] == '-' else row[2]),
                timestamp=datetime.strptime(row[3], date_format_input),
                http_method=row[4],
                resource=("None" if row[5] in ("-", " ", "") else row[5]),
                http_response_status=row[6],
                http_response_size=(None if row[7] == '-' else row[7]),
                referer=(None if row[-2] == '-' else row[-2]),
                user_agent_string=(None if row[-1] == '-' else row[-1]),
                votes=0,
                voted_by=[]
            )
            insert_a_log(dict(log_object))


def insert_hdfs():
    log_object: ReplicateLog

    with open(fsnamesystem_logs, "r", newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader, None)
        for row in reader:
            log_object = ReplicateLog(
                timestamp=datetime.strptime(row[0], date_format_input),
                block_ids=row[2].split(),
                ip=row[3],
                destination_ip=row[4].split(),
                votes=0,
                voted_by=[]
            )

            insert_a_log(dict(log_object), False)


def convert_to_datehour(timestamp: str) -> str:
    datetime_object = datetime.strptime(timestamp, date_format_input)
    return datetime_object.strftime(date_format_output)


def insert_a_log(log, is_access: bool = True):
    if log["log_type"] == "access":
        collection_ref.update_one(
            filter={'referer': log["referer"]},
            update={
                "$addToSet": {"resources": log["resource"]}
            },
            upsert=True,
        )

    if is_access:
        result = collection_access.insert_one(log)
    else:
        result = collection_hdfs.insert_one(log)

    return result


if __name__ == "__main__":
    main()

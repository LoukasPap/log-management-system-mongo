import csv
import random
from models import *
from faker import Faker

# PyMongo Client
from pymongo import MongoClient

client = MongoClient("localhost", 27017)
db = client["NoSQL-LOGS"]
collection_access = db["access_logs"]
collection_hdfs = db["hdfs_logs"]
collection_admins = db["admins"]
collection_ref = db["referers"]

date_format_input = "%y%m%d %H%M%S"
date_format_output = "%y%m%d%H"

access_logs: str = "./logs/access_log_full.csv"
datacxeiver_logs: str = "./logs/hdfs_dataxceiver.csv"
fsnamesystem_logs: str = "./logs/hdfs_fsnamesystem.csv"

TOTAL_LOGS: int = 2_193_031

def generate_admins():
    fake = Faker(locale="el_GR")

    generated_admins: list[Admin] = []
    for i in range(900):
        prof = fake.profile(["username", "mail"])

        generated_admins.append(Admin(
            username=prof["username"],
            email=prof["mail"],
            telephone=fake.phone_number(),
        ))

    return generated_admins


def main():
    generated_admins: list[Admin] = generate_admins()
    collection_admins.delete_many({})
    collection_admins.insert_many([dict(a) for a in generated_admins])

    admins: list = [doc for doc in collection_admins.find()]

    collection_access.delete_many({})
    insert_access(admins)
    # insert_hdfs(fsnamesystem_logs, False)
    # insert_hdfs(datacxeiver_logs, True)


def insert_access(all_admins: list):
    counter: int = 1

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

            if counter == 3:
                counter = 1

                rnd_votes = random.randrange(1, 30)
                rnd_sample = random.sample(all_admins, rnd_votes)

                log_object.voted_by = [a["username"] for a in rnd_sample]
                log_object.votes = rnd_votes

                response = insert_a_log(dict(log_object))

                for admin in rnd_sample:
                    collection_admins.update_one(
                        {"_id": admin["_id"]},
                        {
                            "$push": {"voted_logs": response.inserted_id},
                            "$inc": {"votes_count": 1},
                            "$addToSet": {"voted_ips": row[0]}
                        }
                    )
                # print(response.inserted_id)

            else:
                counter += 1
                response = insert_a_log(dict(log_object))




    print('Inserted access logs')


def insert_hdfs(log_path: str, is_dataxceiver: bool):
    log_object: HadoopFSLog

    with open(log_path, "r", newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader, None)
        for row in reader:
            log_object = HadoopFSLog(
                timestamp=datetime.strptime(row[0], date_format_input),
                log_type=row[1],
                block_ids=row[2].split(),
                ip=row[3],
                destination_ip=row[4].split(),
                votes=0,
                voted_by=[]
            )
            if is_dataxceiver:
                log_object.size = int(row[-1])

            insert_a_log(dict(log_object), False)

    print('Inserted hdfs logs')


def convert_to_datehour(timestamp: str) -> str:
    datetime_object = datetime.strptime(timestamp, date_format_input)
    return datetime_object.strftime(date_format_output)


# def insert_admin():
#     collection_admins.insert_many(admins)



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

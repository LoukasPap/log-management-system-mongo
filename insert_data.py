import csv

from database import client, insert_a_log
from models import *

date_format_input = "%y%m%d %H%M%S"
date_format_output = "%y%m%d%H"

access_logs: str = "./logs/access_log_full.csv"
datacxeiver_logs: str = "./logs/hdfs_dataxceiver.csv"
fsnamesystem_logs: str = "./logs/hdfs_fsnamesystem.csv"


def main():
    insert()


def insert():
    access_log: AccessLog
    loop = client.get_io_loop()

    with open(access_logs, "r", newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader, None)
        i = 1
        res = 0
        for row in reader:
            access_log = AccessLog(
                ip=row[0],
                remote_name=(None if row[1] == '-' else row[1]),
                user_id=(None if row[2] == '-' else row[2]),
                timestamp=datetime.strptime(row[3], date_format_input),
                http_method=row[4],
                resource=row[5],
                http_response_status=row[6],
                http_response_size=(None if row[7] == '-' else row[7]),
                referer=(None if row[-2] == '-' else row[-2]),
                user_agent_string=(None if row[-1] == '-' else row[-1]),
                votes=0,
                voted_by=[]
            )
            #

            # print(dict(access_log))
            tmp = insert_a_log(dict(access_log))
            res += tmp.modified_count
            i += 1
        print(i, res)


def convert_to_datehour(timestmp: str) -> str:
    datetime_object = datetime.strptime(timestmp, date_format_input)
    return datetime_object.strftime(date_format_output)


if __name__ == "__main__":
    main()

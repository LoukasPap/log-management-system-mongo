import re
from datetime import datetime


def dataxceiver_to_list(log: str):
    try:
        if "Receiv" in log:
            hdfs_dataxceiver_receiv_pattern: str = r'(\d{6} \d{6}).*(Receiv\w{2,3}).*blk_-?(\d.*) src: \/([\d\.:]*) dest: ' \
                                                   r'\/([\d\.:]*)(?: of size )?(\d*)?'

            result: re.Match = re.search(hdfs_dataxceiver_receiv_pattern, log)
            fields = [result.group(j) for j in range(1, 7)]
            if fields[5] == '': fields[5] = 0
            return fields

        if "Served" in log:
            hdfs_dataxceiver_served_pattern: str = r'(\d{6} \d{6}).*: ([\d\.:]*) (Served).*blk_-?(\d.*) to \/([\d\.:]*)'

            result: re.Match = re.search(hdfs_dataxceiver_served_pattern, log)
            fields = [result.group(j) for j in range(1, 6)]

            fields[1], fields[3] = fields[3], fields[1]
            fields[1], fields[2] = fields[2], fields[1]
            fields.append(0)
            return fields
    except:
        return "Fail"


def access_to_list(log: str):
    try:
        access_log_pattern: str = r'([\d\.]*) (-) (-) \[(.*) -\d{4}\] "(\w{0,7}) (.*) HTTP\/.{3}" (\d{3}) (-|\d{0,' \
                                  r'5}) "(-|.*)" "(-|.*)"'

        result: re.Match = re.search(access_log_pattern, log)
        fields = [result.group(j) for j in range(1, 11)]
        fields[3] = convert_to_timestamp_insert_log(fields[3])
        return fields
    except:
        return "Fail"


def fsnamesystem_to_list(log: str):
    try:
        hdfs_fsnamesystem_pattern: str = r'(\d{6} \d{6}).* ask ([\d\.:]*) to (replicate|delete) blk_-?(\d*).*node\(s\) (.*) ?'

        result: re.Match = re.search(hdfs_fsnamesystem_pattern, log)

        fields = [result.group(j) for j in range(1, 6)]
        fields[1], fields[3] = fields[3], fields[1]
        fields[1], fields[2] = fields[2], fields[1]
        fields[1] = fields[1].title()
        return fields
    except:
        return "Fail"


def convert_to_timestamp_insert_log(dt: str):
    datetime_object = datetime.strptime(dt, '%d/%b/%Y:%H:%M:%S')
    return str(datetime_object.strftime("%y%m%d %H%M%S"))

from time import sleep
from json import dumps
from kafka import KafkaProducer
import re
import datetime


# Define the regular expression pattern to parse the log lines
log_pattern = re.compile(
    r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - "
    r"\[([\w:/]+\s[+\-]\d{4})\] "
    r'"(\w+) (.*?) (HTTP/1\.\d)" (\d{3}) (\d+) '
    r'"(.*?)" "(.*?)"'
)


# Answer to 1.1
def to_dict(line):
    match = log_pattern.match(line)
    if match:
        date_time_str = match.group(2)
        date_time_obj = datetime.datetime.strptime(
            date_time_str, "%d/%b/%Y:%H:%M:%S %z"
        )
        date_time = date_time_obj.isoformat()

        return {
            "ip_address": match.group(1),
            "date_time": date_time,
            "request_type": match.group(3),
            "request_arg": match.group(4),
            "status_code": int(match.group(6)),
            "request_size": int(match.group(7)),
            "referrer": match.group(8),
            "user_agent": match.group(9),
        }
    return None


# Answer to 1.2
def stream_file_lines(filename, kafka_producer):
    with open(filename, "r") as file:
        for line in file:
            log_dict = to_dict(line)
            if log_dict is not None:
                kafka_producer.send(
                    "test_topic", key=log_dict["ip_address"], value=log_dict
                )
                print(f"Sent log entry to test_topic: {log_dict}")

                # This adjusts the rate at which the data is sent. Use a slower rate for testing your code.
                sleep(1)


# We have already setup a producer for you
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: dumps(x).encode("utf-8"),
    key_serializer=lambda x: x.encode("utf-8"),
)

# Top level call to stream the data to kafka topic. Provide the path to the data. Use a smaller data file for testing.
stream_file_lines("./access.log", producer)

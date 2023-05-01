""" kafka producer for Spark_2k.log https://github.com/logpai/loghub
    Author: Sydney May
    Version: v1 (4/23/2023)
"""

from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime

# primary key to be used in data parsing by consumer
global line_id


# parse log file into dictionary based on csv format: https://github.com/logpai/logparser/blob/master/logs/Spark/Spark_2k.log_structured.csv
def to_dict(line):
    global line_id
    line_dict = {}

    line_arr = line.split(" ")
    content = ""

    for i in range(4, len(line_arr)):
        content += line_arr[i] + " "

    line_dict["LineId"] = str(line_id)

    date_str = line_arr[0]
    time_str = line_arr[1]

    # Combine date and time strings, then parse them into a datetime object
    combined_datetime = datetime.strptime(f"{date_str} {time_str}", "%y/%m/%d %H:%M:%S")

    # Convert the datetime object into an ISO format string
    combined_datetime_str = combined_datetime.isoformat()

    line_dict["DateTime"] = combined_datetime_str

    line_dict["Level"] = line_arr[2]
    line_dict["Component"] = line_arr[3][: len(line_arr[3]) - 1]
    line_dict["Content"] = content

    return line_dict


def stream_file_lines(filename, kafka_producer):
    global line_id
    f = open(filename)
    for line in f:
        # renamed the topic from Assignment 4, hopefully this doesn't cause any issues
        kafka_producer.send("log_topic", key=str(line_id), value=to_dict(line))
        # print(f"Sent {line} to log_topic")
        print(f"Sent log entry to log_topic: {to_dict(line)}")

        # increment line id
        line_id += 1

        # This adjusts the rate at which the data is sent. Use a slower rate for testing your code.
        sleep(0.1)


# We have already setup a producer for you
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: dumps(x).encode("utf-8"),
    key_serializer=lambda x: x.encode("utf-8"),
)

# Top level call to stream the data to kafka topic. Provide the path to the data. Use a smaller data file for testing.
line_id = 1
stream_file_lines("data/Spark_2k.log", producer)

from time import sleep
from json import dumps
import time
from kafka import KafkaProducer
import re


# Answer to 1.1
def to_dict(line):
    regex = r'(?P<ip_address>\d+\.\d+\.\d+\.\d+) - - \[(?P<date_time>[^\]]+)\] "(?P<request_type>\S+) (?P<request_arg>\S+) \S+" (?P<status_code>\d+) (?P<request_size>\d+) "(?P<referrer>[^"]*)" "(?P<user_agent>[^"]*)"'
    match = re.match(regex, line)
    if match:
        return match.groupdict()
    else:
        return {}
    

# Answer to 1.2
def stream_file_lines(file_path, producer_obj):
    with open(file_path, 'r') as f:
        for line in f:
            # process the line to get a dictionary representation of the data
            data_dict = to_dict(line.strip())
            # send the data to the Kafka topic
            producer_obj.send('topic_test', key=data_dict['ip_address'], value=data_dict)
            # sleep for 1 second to control the rate of data streaming
            time.sleep(1)

# We have already setup a producer for you
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'),
    key_serializer=lambda x: x.encode('utf-8')
)
# Top level call to stream the data to kafka topic. Provide the path to the data. Use a smaller data file for testing.
#stream_file_lines(r"C:\Users\Owner\Downloads\cs5614-assignment4-main\archive\short.access.log", producer)
stream_file_lines(r"C:\Users\Owner\Downloads\cs5614-assignment4-main\archive\test.access.log", producer)

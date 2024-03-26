import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from confluent_kafka import Producer
from dotenv import load_dotenv
import time
import datetime
import json
import random
from utils.message_uid import generate_message, generate_user_id


load_dotenv()

primary_kafka_bootstrap_server = f"{os.getenv('PRIMARY_KAFKA_SERVER_IP')}:{
    os.getenv('PRIMARY_KAFKA_SERVER_PORT')}"


def publish_load(topic_name="test", message="Dummy Message From WorkLoad Generator Script!", partition=0):
    producer_config = {'bootstrap.servers': primary_kafka_bootstrap_server}
    producer = Producer(producer_config)
    producer.produce(topic_name, message.encode('utf-8'), partition=partition)
    producer.flush()


if __name__ == "__main__":
    with open(r'config\config_json\primary-kafka-cluster-configuration.json', 'r') as file:
        data = json.load(file)
        # print(data)
        # print(list(data))
        # print(random.choice(list(data)))
    uid = generate_user_id()

    while True:
        message = {"uid": uid}

        random_topic = random.choice(list(data))
        partition = random.randrange(data[random_topic]["num_partitions"])

        # print("Message", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        message_data = f"{generate_message()} {datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")}"
        message["message_data"] = message_data

        if random_topic in ["topic_c", "topic_d"]:
            message["message_destination"] = random.choice(
                ["topic_x", "topic_y"])
            message["priority"] = random.randrange(2)

        message = json.dumps(message)

        print(random_topic, message, partition)
        publish_load(random_topic, message, partition)

        time.sleep(10/60)

from confluent_kafka import Producer
from WorkLoadGenerator import generate_work
from dotenv import load_dotenv
import os
import time, datetime



load_dotenv()

primary_kafka_bootstrap_server = f"{os.getenv('PRIMARY_KAFKA_SERVER_IP')}:{
    os.getenv('PRIMARY_KAFKA_SERVER_PORT')}"


def publish_load(topic_name="test", message="Dummy Message From WorkLoad Generator Script!", partition=0):
    producer_config = {'bootstrap.servers': primary_kafka_bootstrap_server}
    producer = Producer(producer_config)
    producer.produce(topic_name, message.encode('utf-8'), partition=partition)
    producer.flush()


if __name__ == "__main__":
    while True:
        # print("Message", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        message = f"{generate_work()} {datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")}"
        print(message)
        publish_load(message=message)
        time.sleep(1/60)


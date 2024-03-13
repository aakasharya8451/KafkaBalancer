from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import os
import time
from KafkaGatewayRoutLogic import route


load_dotenv()

primary_kafka_bootstrap_server = f"{os.getenv('PRIMARY_KAFKA_SERVER_IP')}:{
    os.getenv('PRIMARY_KAFKA_SERVER_PORT')}"


def consume_messages(topics=["test"], group_id="group-a"):
    print(f"Consumer From Group {group_id} and subscribed for topic {topics}")

    consumer_config = {
        'bootstrap.servers': primary_kafka_bootstrap_server,
        'group.id': group_id,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False
    }

    consumer = Consumer(consumer_config)

    consumer.subscribe(topics)

    join_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=-1)  # -1 for real-time

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f'Consumer error: {msg.error()}')
                    break

            # print(f"Received message: {msg.value().decode(
            #     'utf-8')} from topic: {msg.topic()} partition: {msg.partition()} ")
            message_timestamp = msg.timestamp()[1] / 1000.0
            if message_timestamp > join_time:
                route(f"Received message: {msg.value().decode(
                    'utf-8')} from topic: {msg.topic()} partition: {msg.partition()} ")

    except KeyboardInterrupt:
        consumer.close()


if __name__ == "__main__":
    consume_messages(topics=["topic_c","topic_d"])

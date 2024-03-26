from confluent_kafka import Consumer, KafkaError, Producer
from dotenv import load_dotenv
import os
import time
from src.gateway.routing_logic import route


load_dotenv()

primary_kafka_bootstrap_server = f"{os.getenv('PRIMARY_KAFKA_SERVER_IP')}:{
    os.getenv('PRIMARY_KAFKA_SERVER_PORT')}"

intermediate_kafka_bootstrap_server = f"{os.getenv('INTERMEDIATE_KAFKA_SERVER_IP')}:{
    os.getenv('INTERMEDIATE_KAFKA_SERVER_PORT')}"


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def consume_messages(topics=["test"], group_id="group-a"):
    print(f"Consumer From Group {group_id} and subscribed for topic {topics}")

    producer_config = {
        'bootstrap.servers': intermediate_kafka_bootstrap_server}

    producer = Producer(producer_config)

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
            try:
                message_timestamp = msg.timestamp()[1] / 1000.0
                if message_timestamp > join_time:
                    topic_name, message, partition = route(
                        msg.value().decode('utf-8'), msg.topic())
                    print(topic_name, message, partition)
                    producer.produce(topic_name, message.encode(
                        'utf-8'), partition=partition, callback=delivery_report)
                    producer.flush()
                    print("\n"*5)
            except Exception as e:
                print(
                    "Error occurred while processing or publishing message to the intermediate kafka cluster:", e)

    except KeyboardInterrupt:
        consumer.close()


if __name__ == "__main__":
    consume_messages(topics=["topic_c", "topic_d"])

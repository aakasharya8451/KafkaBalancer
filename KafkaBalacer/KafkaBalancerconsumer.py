from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import os


load_dotenv()

intermediate_kafka_bootstrap_server = f"{os.getenv('INTERMEDIATE_KAFKA_SERVER_IP')}:{
    os.getenv('INTERMEDIATE_KAFKA_SERVER_PORT')}"


def consume_messages(topics=["test"], group_id="group-a"):
    print(f"Consumer From Group {group_id} and subscribed for topic {topics}")

    consumer_config = {
        'bootstrap.servers': intermediate_kafka_bootstrap_server,
        'group.id': group_id,
        'auto.offset.reset': 'end'
    }

    consumer = Consumer(consumer_config)

    consumer.subscribe(topics)

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

            print(f"Received message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        consumer.close()


if __name__ == "__main__":
    consume_messages(["topic_x", "topic_y"])
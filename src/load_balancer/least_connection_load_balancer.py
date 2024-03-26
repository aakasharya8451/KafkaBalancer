from confluent_kafka import Consumer, KafkaException, KafkaError, Producer
import threading
import random
from dotenv import load_dotenv
import os
import requests
import time


load_dotenv()

intermediate_kafka_bootstrap_server = f"{os.getenv('INTERMEDIATE_KAFKA_SERVER_IP')}:{
    os.getenv('INTERMEDIATE_KAFKA_SERVER_PORT')}"

dummy_servers = [
    f"http://{os.getenv("PRIVATE_IP")}:5000/", f"http://{os.getenv("PRIVATE_IP")}:5001/"]
kafka_topics = ["topic_x", "topic_y"]

connection_counts = {server: 0 for server in dummy_servers}

consumer_config = {
    'bootstrap.servers': intermediate_kafka_bootstrap_server,
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False
}

consumer = Consumer(consumer_config)


def consume_and_balance():
    join_time = time.time()
    try:
        consumer.subscribe(kafka_topics)

        while True:
            msg = consumer.poll(timeout=-1)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
                
            message_timestamp = msg.timestamp()[1] / 1000.0
            if message_timestamp > join_time:
                # Choose server with least connections
                chosen_server = min(connection_counts, key=connection_counts.get)
                # print(f"Message: {msg.value().decode(
                #     'utf-8')} sent to {chosen_server}")

                # Send message to chosen server
                requests.post(chosen_server, data=msg.value())
                # response = requests.post(chosen_server, data=msg.value())
                # print("Response from dummy server:", response.text)

                # Simulate message processing time
                processing_time = random.randint(1, 5)
                # Increase connection count for chosen server
                connection_counts[chosen_server] += 1
                print(connection_counts)
                # Simulate message processing time
                threading.Timer(processing_time, decrease_connection_count, args=[
                                chosen_server]).start()

    except KafkaException as e:
        print(f"KafkaException: {e}")


def decrease_connection_count(server):
    print(f"Number of requests processed by {
          server}: {connection_counts[server]}")
    connection_counts[server] -= 1


# Start consumer thread
consumer_thread = threading.Thread(target=consume_and_balance)
consumer_thread.daemon = True
consumer_thread.start()

# Keep the main thread running
while True:
    pass



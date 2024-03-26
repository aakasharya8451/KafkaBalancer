from confluent_kafka import Consumer, KafkaException, KafkaError
import threading
import random
from dotenv import load_dotenv
import os
import requests
import time

load_dotenv()

intermediate_kafka_bootstrap_server = f"{os.getenv('INTERMEDIATE_KAFKA_SERVER_IP')}:{
    os.getenv('INTERMEDIATE_KAFKA_SERVER_PORT')}"


class LoadBalancer:
    def __init__(self, topics=["test"], group_id="my_consumer_group", server_port_list =[5000]):
        self.topics = topics
        self.group_id = group_id
        self.server_port_list = server_port_list
        self.server_list = self.get_server_list()
        self.connection_counts = {server: 0 for server in self.server_list}

    def get_server_list(self):
        server_list = []
        for port in self.server_port_list:
            server_list.append(f"http://{os.getenv('PRIVATE_IP')}:{port}")
        return server_list

    def setup_consumer_producer(self):
        try:
            print("Setting up consumer and producer...")
            consumer_config = {
                'bootstrap.servers': intermediate_kafka_bootstrap_server,
                'group.id': self.group_id,
                'auto.offset.reset': 'latest',
                'enable.auto.commit': False
            }
            self.consumer = Consumer(consumer_config)
            self.consumer.subscribe(self.topics)
            print("Consumer setup completed successfully.")
        except Exception as e:
            print(f"Error occurred during consumer setup: {e}")

    def consume_and_balance(self):
        join_time = time.time()
        try:
            while True:
                msg = self.consumer.poll(timeout=-1)

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
                    chosen_server = min(self.connection_counts,
                                        key=self.connection_counts.get)

                    # Send message to chosen server
                    requests.post(chosen_server, data=msg.value())

                    # Simulate message processing time
                    processing_time = random.randint(1, 5)
                    # Increase connection count for chosen server
                    self.connection_counts[chosen_server] += 1

                    # Simulate message processing time
                    threading.Timer(processing_time, self.decrease_connection_count, args=[
                                    chosen_server]).start()

        except KafkaException as e:
            print(f"KafkaException: {e}")

    def decrease_connection_count(self, server):
        print(f"Number of requests processed by {
              server}: {self.connection_counts[server]}")
        self.connection_counts[server] -= 1

    def run(self):
        try:
            self.setup_consumer_producer()

            # Start consumer thread
            consumer_thread = threading.Thread(target=self.consume_and_balance)
            consumer_thread.start()
            print("Load Balancer started successfully.")
            consumer_thread.join()  # Wait for consumer thread to finish
        except Exception as e:
            print(f"Error occurred while starting LoadBalancer: {e}")


if __name__ == "__main__":
    load_balancer = LoadBalancer(topics = ["topic_x", "topic_y"], server_port_list= [5000, 5001])
    load_balancer.run()
import os
from confluent_kafka import Consumer, KafkaError, Producer
from dotenv import load_dotenv
import time
import threading
from routing_logic import route # calling from inside the script
# from src.gateway.routing_logic import route # Use only when calling from outside the module


load_dotenv()

primary_kafka_bootstrap_server = f"{os.getenv('PRIMARY_KAFKA_SERVER_IP')}:{
    os.getenv('PRIMARY_KAFKA_SERVER_PORT')}"

intermediate_kafka_bootstrap_server = f"{os.getenv('INTERMEDIATE_KAFKA_SERVER_IP')}:{
    os.getenv('INTERMEDIATE_KAFKA_SERVER_PORT')}"


class Gateway:
    def __init__(self, topics=["test"], group_id="group-a"):
        self.topics = topics
        self.group_id = group_id

    def setup_consumer_producer(self):
        try:
            print(f"Consumer From Group {
                  self.group_id} and subscribed for topic {self.topics}")

            producer_config = {
                'bootstrap.servers': intermediate_kafka_bootstrap_server}
            self.producer = Producer(producer_config)

            consumer_config = {
                'bootstrap.servers': primary_kafka_bootstrap_server,
                'group.id': self.group_id,
                'auto.offset.reset': 'latest',
                'enable.auto.commit': False
            }
            self.consumer = Consumer(consumer_config)
            self.consumer.subscribe(self.topics)
            print("Consumer and producer setup completed successfully.")
        except Exception as e:
            print(f"Error occurred during consumer and producer setup: {e}")

    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def consume_messages(self):
        join_time = time.time()

        try:
            while True:
                msg = self.consumer.poll(timeout=-1)  # -1 for real-time

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
                        self.producer.produce(topic_name, message.encode(
                            'utf-8'), partition=partition, callback=self.delivery_report)
                        self.producer.flush()
                        print("\n"*5)
                except Exception as e:
                    print(
                        "Error occurred while processing or publishing message to the intermediate kafka cluster:", e)

        except KeyboardInterrupt:
            self.consumer.close()

    def run(self):
        try:
            self.setup_consumer_producer()
            thread = threading.Thread(target=self.consume_messages)
            thread.start()
            print("Gateway started successfully.")
            thread.join()
        except Exception as e:
            print(f"Error occurred while starting Gateway: {e}")


if __name__ == "__main__":
    gateway = Gateway(topics=["topic_c", "topic_d"])
    gateway.run()

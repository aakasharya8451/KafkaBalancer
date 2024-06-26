from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
import os
import json


load_dotenv()

intermediate_kafka_bootstrap_server = f"{os.getenv('INTERMEDIATE_KAFKA_SERVER_IP')}:{
    os.getenv('INTERMEDIATE_KAFKA_SERVER_PORT')}"

admin_client = AdminClient(
    {'bootstrap.servers': intermediate_kafka_bootstrap_server})


def create_topic(topic_name="test", num_partitions=1, replication_factor=1):
    print("Create a new topic.", topic_name)

    topic = NewTopic(topic_name, num_partitions, replication_factor)
    fs = admin_client.create_topics([topic])

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created successfully.")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

    print("Terminating Admin Client")


if __name__ == "__main__":
    with open(r'config\config_json\intermediate-kafka-cluster-configuration.json', 'r') as file:
        data = json.load(file)

    for topics in data:
        # print(topics, data[topics]["num_partitions"],data[topics]["replication_factor"])
        create_topic(topics, data[topics]["num_partitions"],
                     data[topics]["replication_factor"])


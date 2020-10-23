import csv, os, logging
from datetime import datetime
from kafka import KafkaProducer
from json import dumps

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


def process_data_kafka(path_of_data, broker_address, topic_name, **kwargs):
    producer = KafkaProducer(
        bootstrap_servers=[broker_address],
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )
    if producer.bootstrap_connected():

        logger.info("Connected to kafka!")

        with open(path_of_data) as file:

            reader = csv.DictReader(file)
            for row in reader:
                data = dict(row)
                producer.send(topic_name, value=data)
                producer.flush()

        return "Done"
    else:
        logger.error("Kafka connection failed!")

        return "Failed"


if __name__ == "__main__":

    TOPIC_NAME = os.getenv("TOPIC_NAME")

    brokerAddresses = os.getenv("brokerAddresses")

    process_data_kafka("app/data.txt", brokerAddresses, TOPIC_NAME)

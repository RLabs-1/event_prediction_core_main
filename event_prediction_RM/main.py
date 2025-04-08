import os
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_IN = os.getenv("COL_TOPIC")
TOPIC_OUT = os.getenv("RM_TOPIC")
ENCODING = os.getenv("ENCODING", "utf-8")

print("Hello from RM container!", flush=True)

while True:
    try:
        consumer = KafkaConsumer(TOPIC_IN, bootstrap_servers=KAFKA_BROKER, auto_offset_reset="earliest")
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        print("Connected to Kafka! Ranking Model is listening...", flush=True)
        break
    except NoBrokersAvailable:
        print("Kafka not available yet, retrying in 5 seconds...", flush=True)
        time.sleep(5)

for message in consumer:
    collected_data = message.value.decode(ENCODING)
    print(f"Received from {TOPIC_IN}: {collected_data}", flush=True)

    ranked_event = f"High Priority Event: {collected_data}"
    producer.send(TOPIC_OUT, ranked_event.encode(ENCODING))
    print(f"Sent to {TOPIC_OUT}: {ranked_event}", flush=True)

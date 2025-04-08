import os
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_IN = os.getenv("EPT_TOPIC")
TOPIC_OUT = os.getenv("MM_TOPIC")
ENCODING = os.getenv("ENCODING", "utf-8")

print("Hello from Models Manager container!", flush=True)

while True:
    try:
        consumer = KafkaConsumer(TOPIC_IN, bootstrap_servers=KAFKA_BROKER, auto_offset_reset="earliest")
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        print("Connected to Kafka! Models Manager is listening...", flush=True)
        break
    except NoBrokersAvailable:
        print("Kafka not available yet, retrying in 5 seconds...", flush=True)
        time.sleep(5)

for message in consumer:
    processed_log = message.value.decode(ENCODING)
    print(f"Received from {TOPIC_IN}: {processed_log}", flush=True)

    model_decision = f"Using Model X for: {processed_log}"
    producer.send(TOPIC_OUT, model_decision.encode(ENCODING))
    print(f"Sent to {TOPIC_OUT}: {model_decision}", flush=True)

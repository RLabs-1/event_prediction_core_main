import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_OUT = os.getenv("LFR_TOPIC")
ENCODING = os.getenv("ENCODING", "utf-8")

print("Hello from LFR container!", flush=True)

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        print("Connected to Kafka!", flush=True)
        break
    except NoBrokersAvailable:
        print("Kafka not available yet, retrying in 5 seconds...", flush=True)
        time.sleep(5)

while True:
    log_message = "Log from LFR container, sending to EPT container"
    producer.send(TOPIC_OUT, log_message.encode(ENCODING))
    print(f"Sent to {TOPIC_OUT}: {log_message}", flush=True)
    time.sleep(5)

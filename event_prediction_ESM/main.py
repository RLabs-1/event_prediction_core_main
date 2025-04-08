import os
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_IN = os.getenv("MM_TOPIC")
TOPIC_OUT = os.getenv("ESM_TOPIC")
ENCODING = os.getenv("ENCODING", "utf-8")

print("Hello from ESM container!", flush=True)

while True:
    try:
        consumer = KafkaConsumer(TOPIC_IN, bootstrap_servers=KAFKA_BROKER, auto_offset_reset="earliest")
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        print("Connected to Kafka! ESM is listening...", flush=True)
        break
    except NoBrokersAvailable:
        print("Kafka not available yet, retrying in 5 seconds...", flush=True)
        time.sleep(5)

for message in consumer:
    model_decision = message.value.decode(ENCODING)
    print(f"Received from {TOPIC_IN}: {model_decision}", flush=True)

    prediction = f"Predicted event for: {model_decision}"
    producer.send(TOPIC_OUT, prediction.encode(ENCODING))
    print(f"Sent to {TOPIC_OUT}: {prediction}", flush=True)

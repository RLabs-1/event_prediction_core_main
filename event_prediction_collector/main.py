import os
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_IN = os.getenv("LLM_TOPIC")
TOPIC_OUT = os.getenv("COL_TOPIC")
ENCODING = os.getenv("ENCODING", "utf-8")

print("Hello from the Event Prediction Collector!", flush=True)

# Connect to Kafka and listen for AI-enhanced data from `llm-topic`
while True:
    try:
        consumer = KafkaConsumer(TOPIC_IN, bootstrap_servers=KAFKA_BROKER, auto_offset_reset="earliest")
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        print("Connected to Kafka! Collector is listening...", flush=True)
        break
    except NoBrokersAvailable:
        print("Kafka not available yet, retrying in 5 seconds...", flush=True)
        time.sleep(5)

for message in consumer:
    ai_result = message.value.decode(ENCODING)
    print(f"Received from {TOPIC_IN}: {ai_result}", flush=True)

    final_data = f"Collected Data: {ai_result}"
    producer.send(TOPIC_OUT, final_data.encode(ENCODING))
    print(f"Sent to {TOPIC_OUT}: {final_data}", flush=True)

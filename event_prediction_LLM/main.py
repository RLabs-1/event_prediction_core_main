import os
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_IN = os.getenv("ESM_TOPIC")
TOPIC_OUT = os.getenv("LLM_TOPIC")
ENCODING = os.getenv("ENCODING", "utf-8")

print ("Hello from LLM container!", flush=True)

while True:
    try:
        consumer = KafkaConsumer(TOPIC_IN, bootstrap_servers=KAFKA_BROKER, auto_offset_reset="earliest")
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        print("Connected to Kafka! LLM is listening...", flush=True)
        break
    except NoBrokersAvailable:
        print("Kafka not available yet, retrying in 5 seconds...", flush=True)
        time.sleep(5)

for message in consumer:
    prediction = message.value.decode(ENCODING)
    print(f"Received from {TOPIC_IN}: {prediction}", flush=True)

    ai_enhancement = f"AI-enhanced: {prediction}"
    producer.send(TOPIC_OUT, ai_enhancement.encode(ENCODING))
    print(f"Sent to {TOPIC_OUT}: {ai_enhancement}", flush=True)

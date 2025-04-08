import os
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Environment variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_IN = os.getenv("LFR_TOPIC")  
TOPIC_OUT = os.getenv("EPT_TOPIC")
ENCODING = os.getenv("ENCODING")

print("Hello from EPT container!", flush=True)

# Connect to Kafka and listen for log data from `lfr-topic`
while True:
    try:
        consumer = KafkaConsumer(TOPIC_IN, bootstrap_servers=KAFKA_BROKER, auto_offset_reset="earliest", group_id="ept-group")
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        print("Connected to Kafka! EPT is listening for {TOPIC_IN}...", flush=True)
        break
    except NoBrokersAvailable:
        print("Kafka not available yet, retrying in 5 seconds...", flush=True)
        time.sleep(5)

for message in consumer:
    # Extract log data from Kafka message and process it
    log_data = message.value.decode(ENCODING)
    print("Hello i am EPT container ", flush=True)
    print(f"Received from {TOPIC_IN}: {log_data}", flush=True)

    # Process log and forward to `ept-topic`
    processed_log = f"Processed: {log_data}"
    producer.send(TOPIC_OUT, processed_log.encode(ENCODING))   # Sending to `ept-topic`
    print(f"Sent to {TOPIC_OUT}: {processed_log}", flush=True)

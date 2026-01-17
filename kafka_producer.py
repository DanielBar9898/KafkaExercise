from kafka import KafkaProducer
import csv
import json

# Kafka broker
BROKER = 'localhost:9092'
TOPIC = 'electric-cars'

# Create producer
producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Path to your CSV
csv_file_path = r"C:\Users\DanielBA\pyspark\input\Electric_Vehicle_Population_Data.csv"

with open(csv_file_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        producer.send(TOPIC, row)  # send each row as JSON
        print(f"Sent: {row}")

producer.flush()
producer.close()
print("All messages sent!")

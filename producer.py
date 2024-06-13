import time
from confluent_kafka import Producer
import datetime
import json

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092'
}

# Create Producer instance
producer = Producer(**conf)

# Kafka topic
topic = 'test-topic'

# Delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed for message: {msg.key()} - {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

# Publish data every second
value = 0
while True:
    value = value+1
    
    timestamp = int(time.time()*1000)
    message = {'value': value, 'timestamp': timestamp}

    producer.produce(topic, key=str(value), value=json.dumps(message), callback=delivery_report)
    producer.poll(1)
    time.sleep(1)

producer.flush()
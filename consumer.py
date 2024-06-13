from confluent_kafka import Consumer, KafkaError
import time
import json
# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

# Create Consumer instance
consumer = Consumer(**conf)

# Kafka topic
topic = 'test-topic'
consumer.subscribe([topic])

# Poll for new messages
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        else:
            # print(f'Received message: {msg.value().decode("utf-8")}')
            recieveData = json.loads(msg.value().decode("utf-8"))
            print(recieveData,"---------------")

            currentTimeStamp = int(time.time()*1000)
            # print(recieveData['timestamp'])
            # print(recieveData['timestamp'])
            timegape = currentTimeStamp-int(recieveData["timestamp"])
            print(f"latency for value {recieveData['value']} is {timegape/1000} sec")

finally:
    consumer.close()
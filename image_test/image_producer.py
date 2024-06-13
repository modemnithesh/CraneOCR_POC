# image_producer.py

from confluent_kafka import Producer
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

conf = {
    'bootstrap.servers': 'localhost:9092',
    'batch.size': 1048576,  # Increase batch size to 1MB
    'linger.ms': 50,        # Increase linger time to allow batching
    'compression.type': 'snappy',  # Use snappy compression for faster compression/decompression
    'acks': '1',            # Acknowledge on leader
    'queue.buffering.max.messages': 1000000,  # Allow more messages to queue up
    'queue.buffering.max.ms': 1000,           # Max time to buffer messages
    'delivery.report.only.error': True        # Only report errors to reduce callback overhead
}

producer = Producer(**conf)

start_time = time.time()
print(f'Start time: {start_time * 1000}')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

def produce_image_event(topic, key, image_path):
    with open(image_path, 'rb') as image_file:
        image_bytes = image_file.read()
        producer.produce(topic, key=key, value=image_bytes, callback=delivery_report)

# Use ThreadPoolExecutor to send images in parallel
def produce_images_in_parallel(image_folder, topic, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for image_name in os.listdir(image_folder):
            image_path = os.path.join(image_folder, image_name)
            if os.path.isfile(image_path):
                futures.append(executor.submit(produce_image_event, topic, image_name, image_path))
        
        # Ensure all futures are completed
        for future in as_completed(futures):
            future.result()

    # Ensure all messages are delivered
    producer.flush()

# Example usage
if __name__ == "__main__":
    image_folder = r'send_images'
    topic = 'image_topic'

    produce_images_in_parallel(image_folder, topic)
    
    end_time = time.time()
    print(f'End time: {end_time * 1000}')
    print(f'Total time taken: {(end_time - start_time) * 1000} ms')

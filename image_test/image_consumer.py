# image_consumer.py

from confluent_kafka import Consumer, KafkaException
import os
import time

conf = {
    'bootstrap.servers': '55.55.52.178:9092',
    'group.id': 'image_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(**conf)
start_time = time.time()
print(f'Start time: {start_time * 1000}')

def consume_image_events(topic, output_folder):
    consumer.subscribe([topic])
    
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
                    raise KafkaException(msg.error())
            else:
                image_name = msg.key().decode('utf-8')
                image_bytes = msg.value()
                
                output_path = os.path.join(output_folder, image_name)
                with open(output_path, 'wb') as image_file:
                    image_file.write(image_bytes)
                print(f'Saved image: {output_path}')
                print(int(time.time()*1000))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Example usage
if __name__ == "__main__":
    topic = 'image_topic'
    output_folder = r'receive_images'
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    consume_image_events(topic, output_folder)
    end_time = time.time()
    print(f'End time: {end_time * 1000}')
    print(f'Total time taken: {(end_time - start_time) * 1000} ms')
from kafka import KafkaProducer
from time import sleep


producer = KafkaProducer(bootstrap_servers=['localhost:9094'])

producer.send(
    topic='messages',
    value=b'my message from python',
    key=b'python-message',
)

sleep(1)
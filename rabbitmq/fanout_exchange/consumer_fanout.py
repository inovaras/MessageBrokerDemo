from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from pika.adapters.blocking_connection import BlockingChannel
from pika.exchange_type import ExchangeType
from pika.spec import BasicProperties, Basic


EXCHANGE_NAME = 'events'


def handler(ch: BlockingChannel, metchod: Basic.Deliver, properties: BasicProperties, body: bytes) -> None:
    print(f'[🎉] Получено:{body.decode()}')


if __name__ == '__main__':
    credentials = PlainCredentials('user', 'password')
    connection = BlockingConnection(ConnectionParameters(host='localhost', credentials=credentials))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type = ExchangeType.fanout)
    queue = channel.queue_declare(queue='', exclusive=True)

    queue_name = queue.method.queue

    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key='')
    channel.basic_consume(queue=queue_name, on_message_callback=handler, auto_ack=True)
    channel.start_consuming()
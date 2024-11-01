import json

import pika
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.connection import ConnectionParameters
from pika.credentials import PlainCredentials
from pika.spec import BasicProperties, Basic


credentials = PlainCredentials('user', 'password')
connection = BlockingConnection(ConnectionParameters(host='localhost', credentials=credentials))
channel = connection.channel()
channel.queue_declare(queue='rpc_queue')


USERS = {
    0: {'username': 'Александр Стравинский', 'balance': 0},
    1: {'username': 'Степан Лиходеев', 'balance': 0},
    2: {'username': 'Тимофей Квасцов', 'balance': 0},
}

def update_balance_for_user(user_id: int, amount: int) -> dict:
    USERS[user_id]['balance'] += amount
    return USERS[user_id]


def handler(ch: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
    message = json.loads(body)
    print(f'[₽] Увеличение баланса пользователя {message["id"]} на {message["amount"]}')
    updated_user = update_balance_for_user(user_id=message['id'], amount=message['amount'])
    ch.basic_publish(exchange='',routing_key=properties.reply_to, properties=pika.BasicProperties(correlation_id=properties.correlation_id), body=json.dumps(updated_user))
    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    channel.basic_qos(prefetch_count=1) # чтобы запустить более одного серверного процесса. Чтобы равномерно распределить нагрузку на несколько серверов, нужно установить параметр prefetch_count. При запуске нескольких серверов, они стартуют не всегда одновременно, а RabbitMQ по умолчанию отправляет сообщения из очереди первому же доступному консьюмеру. При использовании опции prefetch_count=n [в примере n=1, но может быть и 2, 3, 4, ...] консьюмер не получает следующие n сообщений, пока не подтвердит предыдущие.
    channel.basic_consume(queue='rpc_queue', on_message_callback=handler)
    print('[🎉] Ожидание RPC запроса')
    channel.start_consuming()

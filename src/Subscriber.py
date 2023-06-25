import json
import multiprocessing
import pika

from typing import List

from Config import Config
from src.generator.SubscriptionsGeneratorParallel import SubscriptionsGeneratorParallel


class Subscriber:
    def __init__(self, index: int):
        super().__init__()

        self.index = index
        self.connection = None
        self.channel = None

        self._declare_queue_to_subscribe_to()

    def _declare_queue_to_subscribe_to(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=Config.CONNECTION_HOST,
            port=Config.CONNECTION_PORT
        ))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=Config.SUBSCRIBER_QUEUE_NAME)

        print(f'[Subscriber-{self.index}] Started.')

    def subscribe(self, subscription: dict):
        message_to_send_bytes = json.dumps(subscription, indent=4).encode('utf-8')

        self.channel.basic_publish(
            exchange='',
            routing_key=Config.SUBSCRIBER_QUEUE_NAME,
            body=message_to_send_bytes
        )

        print(f'[Subscriber-{self.index}] Subscribed to broker: {subscription}')

        self._declare_queue_to_receive_from()
        self.channel.start_consuming()

    def _declare_queue_to_receive_from(self):
        self.channel.queue_declare(queue=f'{Config.MATCHING_QUEUE_NAME}-{self.index}')

        self.channel.basic_consume(
            queue=f'{Config.MATCHING_QUEUE_NAME}-{self.index}',
            on_message_callback=self._receive_publication_matched_with_subscription,
            auto_ack=True
        )

    def _receive_publication_matched_with_subscription(self, ch, method, properties, body):
        message = json.loads(body)
        print(f'[Subscriber-{self.index}] Matched publication received: {message}')

    def close(self):
        self.connection.close()


def start_subscriber(index: int, need_complex_subscription: bool):
    subscriber = Subscriber(index)

    generator = SubscriptionsGeneratorParallel(
        100,
        need_complex_subscription,
        1,
        {
            'stationId': 50,
            'city': 50,
            'temperature': 50,
            'rain': 50,
            'wind': 50,
            'direction': 50,
            'date': 50,
        },
        50
    )

    subscription = generator.generate()[0]

    if TEST_SUBSCRIPTIONS:
        if index == 1:  # subscriptie simpla
            subscription = [
                {
                    'field': 'temperature',
                    'operator': '>',
                    'value': 5550
                }
            ]
        if index == 2:  # subscriptie complexa
            subscription = [
                {
                    'field': 'avg_temperature',
                    'operator': '>',
                    'value': -10
                },
                {
                    'field': 'rain',
                    'operator': '!=',
                    'value': '0.05'
                }
            ]
        if index == 3:  # nothing
            subscription = [
                {
                    'field': 'temperature',
                    'operator': '>',
                    'value': 5000
                }
            ]

    subscriber.subscribe({
        'id': index,
        'is_complex': need_complex_subscription,
        'subscription': subscription
    })


TEST_SUBSCRIPTIONS = True


if __name__ == '__main__':
    subscriber_processes = [
        multiprocessing.Process(
            name=f'Subscriber-{index}',
            target=start_subscriber,
            args=(
                index + 1,
                False if index % 2 == 0 else True
            )
        )
        for index in range(Config.NO_SUBSCRIBERS)
    ]

    for subscriber_process in subscriber_processes:
        subscriber_process.start()


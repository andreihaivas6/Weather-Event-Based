import multiprocessing
import time

import pika
import json

from Config import Config


class Broker:
    def __init__(self, index: int):
        super().__init__()

        self.index = index
        self.routing_table: list = list()

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=Config.CONNECTION_HOST,
            port=Config.CONNECTION_PORT
        ))
        self.channel = self.connection.channel()

        self._declare_queues_to_consume_from()

    def _declare_queues_to_consume_from(self):
        self.channel.queue_declare(queue=f'{Config.PUBLISHER_QUEUE_NAME}-{self.index}')
        self.channel.basic_consume(
            queue=f'{Config.PUBLISHER_QUEUE_NAME}-{self.index}',
            on_message_callback=self._callback_consume_from_publisher,
            auto_ack=True
        )

        self.channel.queue_declare(queue=Config.SUBSCRIBER_QUEUE_NAME)

        self.channel.basic_consume(
            queue=Config.SUBSCRIBER_QUEUE_NAME,
            on_message_callback=self._callback_consume_from_subscriber,
            auto_ack=True
        )

    def start_consuming(self):
        print(f'[Broker-{self.index}] Started.')
        self.channel.start_consuming()

    def _callback_consume_from_publisher(self, ch, method, properties, body):
        publication = json.loads(body)

        for pair in self.routing_table:
            subscriber_id = pair['id']
            subscription = pair['subscription']

            if Broker.publication_matches_subscription(publication, subscription):
                self._publish_matched_subscription(publication, subscriber_id)

    def _callback_consume_from_subscriber(self, ch, method, properties, body):
        message = json.loads(body)
        self.routing_table.append(message)

        print(f"[Broker-{self.index}] Received from subscriber: {message}")

    @staticmethod
    def publication_matches_subscription(publication, subscription):
        number_publication = int(publication['publication'].split('-')[1])
        number_subscription = int(subscription['subscription'].split('-')[1])

        return number_publication == number_subscription

    def _publish_matched_subscription(self, publication: dict, subscriber_id: str):
        message_to_send = json.dumps(publication, indent=4).encode('utf-8')

        self.channel.basic_publish(
            exchange='',
            routing_key=f'{Config.MATCHING_QUEUE_NAME}-{subscriber_id}',
            body=message_to_send
        )
        print(f"[Broker-{self.index}] Published to subscriber following matched publication: {publication}, id: {subscriber_id}")

    def __str__(self):
        return f'Broker-{self.index}'


def start_broker(index):
    broker = Broker(index)
    broker.start_consuming()


if __name__ == '__main__':
    broker_processes = [
        multiprocessing.Process(
            name=f'Broker-{index}',
            target=start_broker,
            args=(index + 1,)
        )
        for index in range(Config.NO_BROKERS)
    ]

    for process in broker_processes:
        process.start()


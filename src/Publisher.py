from datetime import datetime

import pika
import json

from google.protobuf.internal.well_known_types import Timestamp

from Config import Config
from src.generator import publication_pb2
from src.generator.PublicationsGeneratorParallel import PublicationsGeneratorParallel


class Publisher:
    def __init__(self):
        self.connection = None
        self.channel = None

        self._declare_queue_to_publish_to()

    def _declare_queue_to_publish_to(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=Config.CONNECTION_HOST,
            port=Config.CONNECTION_PORT
        ))
        self.channel = self.connection.channel()

        for index in range(Config.NO_BROKERS):
            self.channel.queue_declare(queue=f'{Config.PUBLISHER_QUEUE_NAME}-{index + 1}')

        print('[Publisher] Started.')

    def publish(self, message: dict):
        msg = publication_pb2.Publication()
        msg.stationId = message.get('stationId')
        msg.city = message.get('city')
        msg.temperature =message.get('temperature')
        msg.rain = message.get('rain')
        msg.wind = message.get('wind')
        msg.direction = message.get('direction')
        day = message.get('date').split('/')[0]
        month = message.get('date').split('/')[1]
        year = message.get('date').split('/')[2]
        msg.date.day = day
        msg.date.month = month
        msg.date.year = year

        serialized_msg = msg.SerializeToString()

        for index in range(Config.NO_BROKERS):
            self.channel.basic_publish(
                exchange='',
                routing_key=f'{Config.PUBLISHER_QUEUE_NAME}-{index + 1}',
                body=serialized_msg
            )

        print(f'[Publisher] Published to brokers: {message}')

    def close(self):
        self.connection.close()


if __name__ == '__main__':
    publisher = Publisher()

    generator = PublicationsGeneratorParallel(10)
    publications = generator.generate()

    for publication in publications:
        publisher.publish(publication)

    publisher.close()

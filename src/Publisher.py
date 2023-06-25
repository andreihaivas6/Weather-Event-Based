import pika
import json

from Config import Config
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
        message_to_send = json.dumps(message, indent=4).encode('utf-8')

        for index in range(Config.NO_BROKERS):
            self.channel.basic_publish(
                exchange='',
                routing_key=f'{Config.PUBLISHER_QUEUE_NAME}-{index + 1}',
                body=message_to_send
            )

        print(f'[Publisher] Published to brokers: {message}')

    def close(self):
        self.connection.close()


if __name__ == '__main__':
    publisher = Publisher()

    generator = PublicationsGeneratorParallel(1000)
    publications = generator.generate()

    for publication in publications:
        publisher.publish(publication)

    publisher.close()

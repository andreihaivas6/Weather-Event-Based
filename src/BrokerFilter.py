import multiprocessing
import os
import time

import pika
import json
import sched

from google.protobuf.json_format import MessageToJson

from Config import Config
from src.generator import publication_pb2


class BrokerFilter:
    FIELD_WITH_AVG = ['temperature', 'wind', 'rain']

    def __init__(self, index: int):
        super().__init__()

        self.index = index

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=Config.CONNECTION_HOST,
            port=Config.CONNECTION_PORT
        ))
        self.channel = self.connection.channel()

        self._declare_queues_to_consume_from()

    def _declare_queues_to_consume_from(self):
        self.channel.queue_declare(queue=f'{Config.MATCHING_QUEUE_NAME}-{self.index}')
        self.channel.basic_consume(
            queue=f'{Config.MATCHING_QUEUE_NAME}-{self.index}',
            on_message_callback=self._callback_consume_from_broker,
            auto_ack=True
        )

    def start_consuming(self):
        print(f'[Broker-Filter-{self.index}] PID: {multiprocessing.current_process().pid}')
        print(f'[Broker-Filter-{self.index}] Started.')

        self.channel.start_consuming()

    def _callback_consume_from_broker(self, ch, method, properties, body):
        data = json.loads(body)

        if self._is_filtered(data):
            self._publish_filtered_publication(data)

    def _is_filtered(self, publication: dict) -> bool:
        if 'temperature' in publication:
            if publication['temperature'] < -50 or publication['temperature'] > 70:
                return False

        if 'city' not in publication:
            return False
        if publication['city'] not in ['Iasi', 'Roman']:
            return False

        return True

    def _publish_filtered_publication(self, publication: dict):
        message_to_send = json.dumps(publication, indent=4).encode('utf-8')

        self.channel.queue_declare(queue=f'{Config.FILTER_QUEUE_NAME}-{self.index}')

        self.channel.basic_publish(
            exchange='',
            routing_key=f'{Config.FILTER_QUEUE_NAME}-{self.index}',
            body=message_to_send
        )
        print(
            f"[Broker-Filter-{self.index}] Published to subscriber following filtered publication: {publication}, id: {self.index}")

    def __str__(self):
        return f'Broker-Filter-{self.index}'


def start_broker(index):
    broker = BrokerFilter(index)
    broker.start_consuming()


if __name__ == '__main__':
    broker_processes = [
        multiprocessing.Process(
            name=f'Broker-Filter-{index + 1}',
            target=start_broker,
            args=(index + 1,)
        )
        for index in range(Config.NO_BROKERS_FILTER)
    ]

    for index, process in enumerate(broker_processes):
        process.start()


    def check_brokers_alive(scheduler, broker_processes: list):
        scheduler.enter(5, 1, check_brokers_alive, (scheduler, broker_processes, ))
        for index, process in enumerate(broker_processes):
            if not process.is_alive():
                print(f'Process {process.name} is not alive. Restarting...')
                broker_processes[index] = multiprocessing.Process(
                    name=f'Broker-Filter-{index + 1}',
                    target=start_broker,
                    args=(index + 1,)
                )
                broker_processes[index].start()

    my_scheduler = sched.scheduler(time.time, time.sleep)
    my_scheduler.enter(5, 1, check_brokers_alive, (my_scheduler, broker_processes, ))
    my_scheduler.run()

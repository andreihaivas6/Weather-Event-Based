import multiprocessing
import os
import time

import pika
import json
import sched

from google.protobuf.json_format import MessageToJson

from Config import Config
from src.generator import publication_pb2


class Broker:
    FIELD_WITH_AVG = ['temperature', 'wind', 'rain']

    def __init__(self, index: int):
        super().__init__()

        self.time = time.time()
        self.counter = 0

        self.index = index
        self.routing_table: list = list()

        self.last_publications: list = list()

        if os.path.exists(f'broker-{index}-serializer.json'):
            with open(f'broker-{index}-serializer.json', 'r') as file:
                data = json.load(file)
                self.routing_table = data['routing_table']
                self.last_publications = data['last_publications']

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
        print(f'[Broker-{self.index}] PID: {multiprocessing.current_process().pid}')
        print(f'[Broker-{self.index}] Started.')

        self.channel.start_consuming()

    def _callback_consume_from_publisher(self, ch, method, properties, body):
        # publication = json.loads(body)
        new_msg = publication_pb2.MyPublication()
        new_msg.ParseFromString(body)
        json_data = MessageToJson(new_msg)
        json_data = json.loads(json_data)
        json_time = float(json_data['time'])
        json_data = json_data['publication']
        transformed_data = json_data.copy()
        transformed_data['date'] = f"{json_data['date']['day']}/{json_data['date']['month']}/{json_data['date']['year']}"
        with open(f'arriving time-{self.index}.txt', 'a') as f:
            f.write(f'{time.time()-float(json_time)}\n')

        # print(f"[Broker-{self.index}] Received from publisher: {transformed_data}")
        # publication = json.loads(body)
        # print(f"[Broker-{self.index}] Received from publisher: {publication}")

        self.last_publications.append(transformed_data)

        for data in self.routing_table:
            subscriber_id = data['id']
            is_complex = data['is_complex']
            subscription = data['subscription']

            if not is_complex and self._publication_matches_simple_subscription(transformed_data, subscription):
                self._publish_matched_subscription(transformed_data, subscriber_id)

            if is_complex and len(self.last_publications) == Config.WINDOWS_SIZE and self._window_matches_complex_subscription(subscription):
                self._publish_matched_subscription(
                    {
                        'publications': self.last_publications,
                    },
                    subscriber_id
                )
                self.last_publications.clear()

        self.counter += 1
        now = time.time()
        if now - self.time > 3 * 60:
            with open(f'broker-{self.index}.txt', 'w') as f:
                f.write(f'{self.counter}\n')
            exit(0)

    def _callback_consume_from_subscriber(self, ch, method, properties, body):
        message = json.loads(body)
        self.routing_table.append(message)
        self.save_state()

        print(f"[Broker-{self.index}] Received from subscriber: {message}")

    def _publication_matches_simple_subscription(self, publication, subscription) -> bool:
        for data in subscription:
            field = data['field']
            if field not in publication:
                return False

            value_from_publication = publication[field]
            value_from_subscription = data['value']

            if not Broker.condition_between_2_values(
                    value_from_publication, value_from_subscription, data['operator']
            ):
                return False

        return True

    def _window_matches_complex_subscription(self, subscription) -> bool:
        for data in subscription:
            field = data['field']
            value_from_subscription = data['value']

            if not str(field).startswith('avg_'):
                for publication in self.last_publications:
                    if field not in publication:
                        continue
                    value_from_publication = publication[field]

                    if not Broker.condition_between_2_values(
                            value_from_publication, value_from_subscription, data['operator']
                    ):
                        return False
            else:
                average_result = 0
                any_value = False

                for publication in self.last_publications:
                    if field[4:] not in publication:
                        continue
                    average_result += publication[field[4:]] / Config.WINDOWS_SIZE
                    any_value = True

                if not any_value or not Broker.condition_between_2_values(
                        average_result, value_from_subscription, data['operator']
                ):
                    return False

        return True

    def _publish_matched_subscription(self, publication: dict, subscriber_id: str):
        message_to_send = json.dumps(publication, indent=4).encode('utf-8')

        self.channel.basic_publish(
            exchange='',
            routing_key=f'{Config.MATCHING_QUEUE_NAME}-{subscriber_id}',
            body=message_to_send
        )
        print(
            f"[Broker-{self.index}] Published to Broker-Filter following matched publication: {publication}, id: {subscriber_id}")

    @staticmethod
    def condition_between_2_values(value1, value2, operator: str) -> bool:
        return value1 == value2 \
            if operator == '=' \
            else value1 >= value2 \
            if operator == '>=' \
            else value1 > value2 \
            if operator == '>' \
            else value1 <= value2 \
            if operator == '<=' \
            else value1 < value2 \
            if operator == '<' \
            else value1 != value2

    def save_state(self):
        data = {
            'routing_table': self.routing_table,
            'last_publications': self.last_publications
        }
        with open(f'broker-{self.index}-serializer.json', 'w') as fp:
            json.dump(data, fp, indent=4)

    def __str__(self):
        return f'Broker-{self.index}'


def start_broker(index):
    broker = Broker(index)
    broker.start_consuming()


if __name__ == '__main__':
    broker_processes = [
        multiprocessing.Process(
            name=f'Broker-{index + 1}',
            target=start_broker,
            args=(index + 1,)
        )
        for index in range(Config.NO_BROKERS)
    ]

    for index, process in enumerate(broker_processes):
        if os.path.exists(f'broker-{index + 1}-serializer.json'):
            os.remove(f'broker-{index + 1}-serializer.json')
        process.start()


    def check_brokers_alive(scheduler, broker_processes: list):
        scheduler.enter(5, 1, check_brokers_alive, (scheduler, broker_processes, ))
        for index, process in enumerate(broker_processes):
            if not process.is_alive():
                print(f'Process {process.name} is not alive. Restarting...')
                broker_processes[index] = multiprocessing.Process(
                    name=f'Broker-{index + 1}',
                    target=start_broker,
                    args=(index + 1,)
                )
                broker_processes[index].start()

    my_scheduler = sched.scheduler(time.time, time.sleep)
    my_scheduler.enter(5, 1, check_brokers_alive, (my_scheduler, broker_processes, ))
    my_scheduler.run()

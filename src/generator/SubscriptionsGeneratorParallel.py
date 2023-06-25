import json
import os
import random
import time
from multiprocessing import Pool
from typing import List, Dict, Tuple

from src.generator.SubscriptionsGenerator import SubscriptionsGenerator


class SubscriptionsGeneratorParallel:
    def __init__(self, no_subscriptions: int, need_complex_subscription: bool, no_processes: int,
                 frequency_percent_of_fields: dict, frequency_percent_of_equal_on_city: float):
        self.no_subscriptions = no_subscriptions
        self.no_processes = no_processes
        self.frequency_percent_of_fields = frequency_percent_of_fields
        self.frequency_percent_of_equal_on_city = frequency_percent_of_equal_on_city
        self.need_complex_subscription = need_complex_subscription
        self.duration = 0

    def generate_complex_subscription(self, result):
        for subscription in result:
            created = False
            has_temperature = False
            for data in subscription:
                if random.randint(0, 100) >= 25:
                    if data['field'] == 'temperature':
                        data['field'] = 'avg_temperature'
                        created = True
                if random.randint(0, 100) >= 25:
                    if data['field'] == 'wind':
                        data['field'] = 'avg_wind'
                        created = True
                if random.randint(0, 100) >= 25:
                    if data['field'] == 'rain':
                       data['field'] = 'avg_rain'
                       created = True

                if data['field'] == 'temperature':
                    has_temperature = True

            if not created:
                if has_temperature:
                    for data in subscription:
                        if data['field'] == 'temperature':
                            data['field'] = 'avg_temperature'
                else:
                    subscription.append({
                        'field': 'avg_temperature',
                        'operator': random.choice(SubscriptionsGenerator.OPERATORS),
                        'value': random.randint(-20, 50)
                    })
        return result

    def generate(self) -> List[List[Dict]]:
        start = time.time()

        result = self._generate()

        if self.need_complex_subscription:
            result = self.generate_complex_subscription(result)
        end = time.time()
        self.duration = round(end - start, 2)

        if not os.path.exists('data'):
            os.mkdir('data')

        with open('data/subscriptions.json', 'w') as file:
            file.write(json.dumps(result, indent=4))

        with open('data/subscriptions_data.json', 'w') as file:
            file.write(json.dumps(self.get_data_for_testing(), indent=4))

        return result

    def _generate(self) -> List[List[Dict]]:
        params = self._get_params()

        if self.no_processes == 1:
            return SubscriptionsGenerator.generate(params[0])
        else:
            subscriptions = list()

            with Pool(self.no_processes) as pool:
                for result in pool.map(
                        SubscriptionsGenerator.generate,
                        params
                ):
                    subscriptions.extend(result)

            return subscriptions

    def _get_params(self) -> List[List]:
        no_subscriptions_per_instance = self.no_subscriptions // self.no_processes
        no_extra_subscriptions = self.no_subscriptions % self.no_processes
        no_subscriptions_for_first_instance = no_subscriptions_per_instance + no_extra_subscriptions

        fields_frequency = {
            field: round(self.no_subscriptions * (frequency_percent / 100))
            for field, frequency_percent in self.frequency_percent_of_fields.items()
        }

        assert sum(fields_frequency.values()) >= self.no_subscriptions, "Too few percentages of fields."

        no_equal_on_city = round(fields_frequency['city'] * (self.frequency_percent_of_equal_on_city / 100))
        no_equal_on_city_per_instance = no_equal_on_city // self.no_processes
        no_extra_equal_on_city = no_equal_on_city % self.no_processes
        no_equal_on_city_for_first_instance = no_equal_on_city_per_instance + no_extra_equal_on_city

        fields_frequency_per_instance = {
            field: counter // self.no_processes
            for field, counter in fields_frequency.items()
        }

        extra_fields = {
            field: counter % self.no_processes
            for field, counter in fields_frequency.items()
        }

        fields_frequency_for_first_instance = {
            field: counter + extra_fields[field]
            for field, counter in fields_frequency_per_instance.items()
        }

        params_for_instances = [
            [
                no_subscriptions_for_first_instance,
                no_equal_on_city_for_first_instance,
                fields_frequency_for_first_instance,
            ]
        ]
        params_for_instances += [
            [
                no_subscriptions_per_instance,
                no_equal_on_city_per_instance,
                fields_frequency_per_instance,
            ]
            for _ in range(self.no_processes - 1)
        ]

        # daca avem 100% suma procentelor field-urilor si nu se imparte exact nr de subscriptii la nr de field-uri,
        # atunci primul proces ia prea multe subscriptii
        extra_subscriptions = no_subscriptions_for_first_instance - sum(fields_frequency_for_first_instance.values())
        if extra_subscriptions > 0:
            for i in range(extra_subscriptions):
                params_for_instances[i + 1][0] += 1
            params_for_instances[0][0] -= extra_subscriptions

        # pentru valori mici pentru subscriptii si suma procentelor spre 100%,
        # atunci unele procese primesc prea putine field-uri de generat
        # ex: 1, 2 valori pt fiecare field se impart greu la 4 procese - majoritatea se duc la primul proces
        for i in range(1, self.no_processes):
            no_subscriptions_per_curr_instance = params_for_instances[i][0]
            fields_frequency_per_curr_instance = params_for_instances[i][2]

            extra_subscriptions = no_subscriptions_per_curr_instance - sum(fields_frequency_per_curr_instance.values())
            if extra_subscriptions > 0:
                params_for_instances[0][0] += extra_subscriptions
                params_for_instances[i][0] -= extra_subscriptions

        # s-ar pierde 1, 2 equal pe city in cazuri foarte rare
        # un caz rar de genul: 2, 1, 1, 1 - city pe fiecare proces si 3 city sa aiba equal
        # 3 nu se imparte la 4 procese, deci primul proces le-ar fi luat pe toate
        extra_subscriptions = no_equal_on_city_for_first_instance - fields_frequency_for_first_instance['city']
        if extra_subscriptions > 0:
            for i in range(extra_subscriptions):
                params_for_instances[i + 1][1] += 1
            params_for_instances[0][1] -= extra_subscriptions

        return params_for_instances

    def get_data_for_testing(self):
        return {
            'no_subscriptions': self.no_subscriptions,
            'no_processes': self.no_processes,
            'duration (sec)': self.duration,
        }

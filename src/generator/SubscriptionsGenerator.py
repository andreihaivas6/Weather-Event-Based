import random

from typing import List, Dict, Tuple

from src.PublicationsGenerator import PublicationsGenerator


class SubscriptionsGenerator:
    OPERATORS = [
        '=', '>', '<', '>=', '<=', '!='
    ]

    @staticmethod
    def generate(data: List) -> List[List[Dict]]:  # [int, int, dict]
        no_subscriptions, no_equal_on_city, frequency_of_fields = data

        subscriptions = list()

        index = 0
        while index < no_subscriptions:
            subscription = list()

            for field, field_frequency in frequency_of_fields.items():
                if len(subscription) != 0 and (no_subscriptions - index - 1) == sum(frequency_of_fields.values()):
                    break

                probability_of_picking_field = field_frequency / (no_subscriptions - index)

                if random.random() < probability_of_picking_field:
                    data = {
                        'field': field,
                        'operator': SubscriptionsGenerator.pick_operator(
                            field,
                            no_equal_on_city,
                            field_frequency
                        ),
                        'value': SubscriptionsGenerator.pick_value(field),
                    }
                    if field == 'city' and data['operator'] == '=':
                        no_equal_on_city -= 1

                    frequency_of_fields[field] -= 1
                    subscription.append(data)

            if len(subscription) == 0:
                index -= 1
            else:
                subscriptions.append(subscription)

            index += 1

        return subscriptions

    @staticmethod
    def pick_operator(field: str, no_equal_on_city: int, field_frequency: int) -> str:
        if field != 'city' or no_equal_on_city == 0:
            return random.choice(SubscriptionsGenerator.OPERATORS)

        probability = field_frequency / no_equal_on_city
        if random.random() <= probability:
            return '='
        return random.choice(SubscriptionsGenerator.OPERATORS)

    @staticmethod
    def pick_value(field):
        return random.randint(0, 100) \
            if field == 'stationId' \
            else random.choice(PublicationsGenerator.CITIES) \
            if field == 'city' \
            else random.randint(-20, 50) \
            if field == 'temperature' \
            else round(random.random(), 2) \
            if field == 'rain' \
            else random.randint(0, 90) \
            if field == 'wind' \
            else random.choice(PublicationsGenerator.DIRECTIONS) \
            if field == 'direction' \
            else PublicationsGenerator.get_random_date() \
            if field == 'date' \
            else None

"""
Output:
    result = [
        [
            {
                'field': 'city',
                'operator': '=',
                'value': 'Iasi',
            },
            {
                'field': 'temp',
                'operator': '>=',
                'value': 20,
            },
            ...
        ],
        ...
    ]
"""
import json

from SubscriptionsGeneratorParallel import SubscriptionsGeneratorParallel

NO_SUBSCRIPTIONS = 200_000
NO_PARALLEL_INSTANCES = 4

FREQUENCY_PERCENT_OF_FIELDS = {
    'stationId': 50,
    'city': 50,
    'temperature': 0,
    'rain': 20,
    'wind': 0,
    'direction': 0,
    'date': 0,
}

MIN_FREQUENCY_OF_EQUAL_ON_CITY = 60


def test_no_tuples():
    no_tuples_to_have = 0
    for field, percent in FREQUENCY_PERCENT_OF_FIELDS.items():
        no_tuples_to_have += round(NO_SUBSCRIPTIONS * (percent / 100))

    with open('data/subscriptions.json', 'r') as file:
        subscriptions = json.load(file)
        no_tuples_in_file = sum([
            len(subscription)
            for subscription in subscriptions
        ])

        assert no_tuples_to_have == no_tuples_in_file, \
            f'Expected {no_tuples_to_have} tuples, got {no_tuples_in_file}.'


def test_no_subscriptions():
    with open('data/subscriptions.json', 'r') as file:
        subscriptions = json.load(file)
        assert len(subscriptions) == NO_SUBSCRIPTIONS, \
            f'Expected {NO_SUBSCRIPTIONS} subscriptions, got {len(subscriptions)}.'


def test_no_empty_subscriptions():
    with open('data/subscriptions.json', 'r') as file:
        subscriptions = json.load(file)
        for subscription in subscriptions:
            assert len(subscription), "We got an empty subscription."


def test_no_equal_on_city():
    no_cities = round(NO_SUBSCRIPTIONS * (FREQUENCY_PERCENT_OF_FIELDS['city'] / 100))
    min_no_cities_with_equal = round(no_cities * (MIN_FREQUENCY_OF_EQUAL_ON_CITY / 100))

    with open('data/subscriptions.json', 'r') as file:
        subscriptions = json.load(file)
        no_equal_on_city_on_file = 0
        for subscription in subscriptions:
            for data in subscription:
                if data['field'] == 'city' and data['operator'] == '=':
                    no_equal_on_city_on_file += 1
        assert no_equal_on_city_on_file >= min_no_cities_with_equal, \
            f'Expected at least {min_no_cities_with_equal} equal on city, got {no_equal_on_city_on_file}.'


def test_subscriptions():
    test_no_tuples()
    test_no_subscriptions()
    test_no_empty_subscriptions()
    test_no_equal_on_city()


if __name__ == '__main__':
    generator = SubscriptionsGeneratorParallel(
        NO_SUBSCRIPTIONS,
        NO_PARALLEL_INSTANCES,
        FREQUENCY_PERCENT_OF_FIELDS,
        MIN_FREQUENCY_OF_EQUAL_ON_CITY
    )
    result = generator.generate()

    print(json.dumps(generator.get_data_for_testing(), indent=4))

    test_subscriptions()

class Config:
    CONNECTION_HOST = 'localhost'
    CONNECTION_PORT = 5672

    PUBLISHER_QUEUE_NAME = 'publisher_queue_name'
    PUBLISHER_ROUTING_KEY = 'publisher_routing_key'
    PUBLISHER_EXCHANGE_NAME = 'publisher_exchange_name'

    SUBSCRIBER_QUEUE_NAME = 'subscriber_queue_name'
    SUBSCRIBER_ROUTING_KEY = 'subscriber_routing_key'
    SUBSCRIBER_EXCHANGE_NAME = 'subscriber_exchange_name'

    MATCHING_QUEUE_NAME = 'matching_queue_name'
    MATCHING_ROUTING_KEY_PREFIX = 'matching_routing_key'
    MATCHING_EXCHANGE_NAME = 'matching_exchange_name'

    WINDOWS_SIZE = 10

    NO_BROKERS = 2
    NO_SUBSCRIBERS = 3
    NO_PUBLICATIONS = 10_000

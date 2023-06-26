class Config:
    CONNECTION_HOST = 'localhost'
    CONNECTION_PORT = 5672

    PUBLISHER_QUEUE_NAME = 'publisher_queue_name'

    SUBSCRIBER_QUEUE_NAME = 'subscriber_queue_name'

    MATCHING_QUEUE_NAME = 'matching_queue_name'

    FILTER_QUEUE_NAME = 'filter_queue_name'

    WINDOWS_SIZE = 5

    NO_BROKERS = 2
    NO_SUBSCRIBERS = 3
    NO_BROKERS_FILTER = NO_SUBSCRIBERS
    NO_PUBLICATIONS = 10_000

class Config:
    CONNECTION_HOST = 'localhost'
    CONNECTION_PORT = 5672

    PUBLISHER_QUEUE_NAME = 'publisher_queue_name'

    SUBSCRIBER_QUEUE_NAME = 'subscriber_queue_name'

    MATCHING_QUEUE_NAME = 'matching_queue_name'

    WINDOWS_SIZE = 10

    NO_BROKERS = 2
    NO_SUBSCRIBERS = 3
    NO_PUBLICATIONS = 10_000

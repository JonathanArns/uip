from confluent_kafka import Consumer, KafkaError
from termcolor import cprint as cp
import sys

def open_connection_to_broker(topic_name='aggregated_pipe', bootstrap_server='kafka:29092'):
    """
    function to open a connection to the kafka broker topic.
    if no system arguments are given a connection to \'kafka:29092\' will be opened
    and the consumer will read from the \'aggregated_pipe\' topic.
    The script runs a while loop to keep the connection open and will read from the stream each every 0.5 seconds.

    param:: topic_name:string, bootstrap_server:string
    return:: -
    raises:: KafkaError
    """
    settings = {
        'bootstrap.servers': bootstrap_server,
        'group.id': 'test_group1',
        'default.topic.config': {
                'auto.offset.reset': 'smallest'
                }
        }

    consumer = Consumer(settings)
    consumer.subscribe([topic_name])


    while True:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        elif not msg.error():
            print('Received message: {0}'.format(msg.value()))
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))


if __name__ == '__main__':
    
    try:
        if len(sys.argv) >= 1:
            topic_name = sys.argv[1]
            bootstrap_server = sys.argv[2]
            open_connection_to_broker(
                            topic_name=topic_name,
                            bootstrap_server=bootstrap_server)
        else:
            open_connection_to_broker() #opens connection with default topic and default bootstrap server
    except KafkaError as kafka_error:
        print(cp(f'log[ERROR]: {kafka_error}'))
        
        
    
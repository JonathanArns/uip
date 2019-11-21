from confluent_kafka import Consumer, KafkaError
import sys

def open_connection_to_broker(topic_name, bootstrap_server):
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
    topic_name = sys.argv[1]
    bootstrap_server = sys.argv[2]

    try:
        open_connection_to_broker(topic_name, bootstrap_server)
    except KafkaError as kafka_error:
        print(str(kafka_error))
    finally:
        pass
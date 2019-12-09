from confluent_kafka import Consumer, Producer, KafkaError


class KafkaConsumer:
    """
    Class Consumer inherits from confluent_kafka.Consumer. As such it acts like the super class
    TODO: write docs for no super methods
    """
    def __init__(self, input_topic, bootstrap_server):
        self.topic = input_topic
        self.bootstrap_server = bootstrap_server
        self.consumer = KafkaConsumer(self._create_settings())
        self.subscribe([input_topic])

    def _create_settings(self):
        """
        creates settings required for the Producer
        return settings:dictonary{}
        """
        return {
            'bootstrap.servers': self.bootstrap_server,
            'group.id': 'test_group1',
            'default.topic.config': {
                    'auto.offset.reset': 'smallest'
                    }
            }

    def _poll_messages(self):
        """
        function which polls messages from the specified kafka topic and returns the message
        param:: -
        return:: message:string
        raise: KafkaEroor if polling fails
        """
        try:
            return self.consumer.poll(0.5)
        except KafkaError as consumer_poll_error:
            raise  consumer_poll_error



class KafkaProducer:
    """
    Class Producer inherits from confluent_kafka.Producer. As such it acts like the super class
    TODO: write docs for no super methods
    """
    def __init__(self, output_topic, bootstrap_server):
        self.topic            = output_topic
        self.bootstrap_server = bootstrap_server
        self.producer = KafkaProducer(self._create_setting())

    def _create_setting(self):
        """
        creates settings required for the Producer
        return settings:dictonary{}
        """
        return {
                'bootstrap.servers': self.bootstrap_server
            }

    def _write_to_topic(self, data):
        """
        function to write to the kafka topic specified in the __init__()
        param:: -
        return:: -
        raises KafkaError if messages faild to publish to topic
        """
        try:
            self.producer.produce(self.topic, data.encode('utf-8'))
        except KafkaError as producer_write_error:
            raise producer_write_error
        finally:
            self.flush()


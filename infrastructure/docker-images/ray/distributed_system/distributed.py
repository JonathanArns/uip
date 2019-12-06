import ray
import json
from .consumer_producer import KafkaConsumer, KafkaProducer
from time import sleep

@ray.remote
def write_results(message_queue_actor, output_topic, bootstrap_server):
    """
    Periodically polls the MessageQueueActor and writes outgoing messages to Kafka.\n
    param:: message_queue_actor:MessageQueueActor, output_topic:string, bootstrap_server:string
    """
    print('IN WRITE')
    producer = KafkaProducer(output_topic, bootstrap_server)
    print('PRODUCER CREATED')
    while True:
        while ray.get(message_queue_actor.hasNext.remote()):
            data = ray.get(message_queue_actor.next.remote())
            producer._write_to_topic(data)
        sleep(1)

def compute(message_queue_actor, model, input_topic, bootstrap_server):
    """
    Creates a number of ModelActors and then periodically polls Kafka and starts a prediction for each message.\n
    param:: message_queue_actor:MessageQueueActor, model:LSTM input_topic:string, bootstrap_server:string
    """
    print('IN COMPUTE')
    consumer = KafkaConsumer(input_topic, bootstrap_server)
    print('CONSUMER CREATED')
    model_obj_id = ray.put(model)
    models = []
    model_index = 0
    for _ in range(1):
        modelActor = ModelActor.remote(message_queue_actor, model_obj_id)
        models.append(modelActor)

    while True:
        msg = consumer.poll(1.0)

        if(msg == None):
            sleep(1)
        else:
            models[model_index].predict.remote(msg.value())
            model_index = (model_index + 1) % len(models)

@ray.remote
class MessageQueueActor():
    """
    Implements a simple queue that can be accessed from everywhere within the ray cluster.
    Buffers outgoing messages to Kafka.
    """
    def __init__(self):
        self.messages = []

    def push(self, message):
        """
        Appends a message at the end of the queue.\n
        param:: message:string
        """
        self.messages.append(message)

    def next(self):
        """
        Removes and returns the first element in the queue.\n
        return:: string
        """
        return self.messages.pop(0)

    def hasNext(self):
        """
        Returns True, if a message is in the queue.\n
        return:: bool
        """
        return len(self.messages) > 0

@ray.remote
class ModelActor():
    """
    A microservice that holds a model.
    Creating multiple ModelActors allows for parallel predictions.
    """
    def __init__(self, message_queue_actor, model_obj_id):
        print(type(model_obj_id))
        self.model = model_obj_id
        self.message_queue = message_queue_actor

    def predict(self, features):
        """
        Calls predict on this Actor's model and writes the result to the MessageQueueActor self.message_queue.\n
        param:: features:string (json)
        """
        prediction = self.model.predict(features)
        for msg in prediction:
            self.message_queue.push.remote(json.dumps(msg))

def run(model, input_topic, output_topic, bootstrap_server):
    """
    Starts the system.\n
    param:: model:LSTM input_topic:string, output_topic:string, bootstrap_server:string
    """
    ray.init('ray-head:6379')
    print('RAY INIT')
    message_queue_actor = MessageQueueActor.remote()
    write_results.remote(message_queue_actor, output_topic, bootstrap_server)
    print('Before compute')
    compute(message_queue_actor, model, input_topic, bootstrap_server)

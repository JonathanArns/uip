import ray
from confluent_kafka import Consumer, KafkaError
from time import sleep

@ray.remote
def write_results(message_queue_actor):
    """create producer and poll the MessageQueueActor periodically for new messages"""
    producer = None # TODO: create kafka producer
 
    while True:
        while ray.get(message_queue_actor.hasNext.remote()):
            pass # TODO: write ray.get(messageQueueActor.next.remote()) to topic
        sleep(1)

@ray.remote
def compute(message_queue_actor):
    """poll periodically, make a prediction and write the result to the message queue Actor"""
    consumer = None # TODO: create kafka consumer
    models = []
    model_index = 0
    for _ in range(2):
        model = ModelActor(message_queue_actor)
        models.append(model)

    while True:
        msg = consumer.poll(1.0)
        if(msg == None):
            sleep(1)
        else:
            models[model_index].predict.remote(msg.value)
            model_index = (model_index + 1) % len(models)

@ray.remote
class MessageQueueActor():
    def __init__(self):
        self.messages = []

    def push(self, message):
        self.messages.append(message)

    def next(self):
        return self.messages.pop(0)

    def hasNext(self):
        return len(self.messages) > 0

@ray.remote
class ModelActor():
    def __init__(self, message_queue_actor):
        import keras
        self.model = keras.models.load_model('./model.h5')
        self.message_queue = message_queue_actor

    def predict(self, features):
        prediction = self.model.predict(features, batch_size=1)
        json = "" # TODO: create json message
        self.message_queue.push.remote(json)

def run():
    ray.init()
    message_queue_actor = MessageQueueActor()
    write_results.remote(message_queue_actor)
    compute.remote(message_queue_actor)

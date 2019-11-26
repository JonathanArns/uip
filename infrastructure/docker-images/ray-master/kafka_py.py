import sys, os
sys.path.append('/usr/src/app/')
from distributed_system import distributed
from distributed_system.lstm.model_creation import LSTM


def run(input_topic, output_topic, bootstrap_server, lstm_model):
    """
    this function will be called as soon as the docker container has started.
    It will call the init() function of ray in order to build a cluster for balancing the workers and actors on the distrebuted system.
    The parameter input_topic describs the kafka topic from which to consume and the parameter output_topic descirbse the kafka topic to write 
    the results to. bootstrap_server is required in order to identiy the broker to connect to
    param:: input_topic:string, output_topic:string, bootstrap_server:string
    return:: -
    """
    distributed.run(None, input_topic, output_topic, bootstrap_server)
    

if __name__ == '__main__':
    arguments = sys.argv
    if not arguments: raise Exception
    
    lstm_model = LSTM()
    run(
        input_topic=arguments[1],
        output_topic=arguments[2],
        bootstrap_server=arguments[3],
        lstm_model=lstm_model
        )
        
    
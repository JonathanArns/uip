import sys, os
import argparse
sys.path.append('/usr/src/app/')
from distributed_system import distributed
from distributed_system.lstm.model_creation import LSTM


def run(input_topic, output_topic, bootstrap_server, lstm_model):
    """
    this function will be called as soon as the docker container has started.
    It will call the init() function of ray in order to build a cluster for balancing the workers and actors on the distrebuted system.
    The parameter input_topic describs the kafka topic from which to consume and the parameter output_topic descirbse the kafka topic to write
    the results to. bootstrap_server is required in order to identiy the broker to connect to
    param:: input_topic:string, output_topic:string, bootstrap_server:string, lstm_model:keras.models.LSTM
    return:: -
    """
    distributed.run(
            lstm_model,
            input_topic,
            output_topic,
            bootstrap_server
            )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-in", "--input", type=str, help="sets the input topic for kafka cunsumer")
    parser.add_argument("-out", "--output", type=str, help="sets the output topic for kafka producer")
    parser.add_argument("-brokder", "--bootstrap", type=str, help="sets the bootstrap server of the broker to connect to")
    args = parser.parse_args()

    lstm_model = LSTM()
    run(
        input_topic=args.input,
        output_topic=args.output,
        bootstrap_server=args.bootstrap,
        lstm_model=lstm_model
        )

from termcolor import cprint as cp
import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from distributed_system import distributed



def run(input_topic, output_topic, bootstrap_server):
    """
    this function will be called as soon as the docker container has started.
    It will call the init() function of ray in order to build a cluster for balancing the workers and actors on the distrebuted system.
    The parameter input_topic describs the kafka topic from which to consume and the parameter output_topic descirbse the kafka topic to write 
    the results to. bootstrap_server is required in order to identiy the broker to connect to
    param:: input_topic:string, output_topic:string, bootstrap_server:string
    return:: -
    """
    

if __name__ == '__main__':
    arguments = sys.argv
    if not arguments: raise Exception

    run(
        input_topic=arguments[1],
        output_topic=arguments[2],
        bootstrap_server=arguments[3]
        )
        
    
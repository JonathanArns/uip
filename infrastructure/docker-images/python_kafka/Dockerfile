FROM python:3.7.4

WORKDIR /usr/src/app

RUN pip install confluent-kafka
COPY kafka_py_consumer.py ./

CMD [ "python3", "-u" ,"./kafka_py_consumer.py", "test", "kafka:29092" ]
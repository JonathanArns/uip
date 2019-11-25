FROM python:3.7.4

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY kafka_py_consumer.py ./

CMD ["ray", "start", "--head", "--redis-port=3301"]
CMD [ "python3", "-u" ,"./kafka_py.py", "input_topic", "output_topic","kafka:29092" ]
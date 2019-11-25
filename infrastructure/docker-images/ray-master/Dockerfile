FROM python:3.7.4

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY kafka_py.py ./
COPY distributed_system ./

CMD ["ray", "start", "--head", "--redis-port=3301"]
CMD ["python3", "-u" ,"./kafka_py.py", "input_topic", "lstm_prediction_results", "kafka:29092"]
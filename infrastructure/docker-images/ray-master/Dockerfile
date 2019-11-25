FROM python:3.7.4

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY kafka_py.py ./
COPY distributed_system ./
COPY init_ray.sh ./

CMD ["bash", "init_ray.sh", "input_topic", "lstm_results", "kafka:29092"]
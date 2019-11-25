FROM python:3.7.4

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY kafka_py_consumer.py ./
COPY init_ray.sh ./
COPY ../../distributed_system/ ./

CMD ["./init_ray.sh", "input_topic", "output_topic", "kafka:29092"]
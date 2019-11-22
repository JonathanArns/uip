FROM python:3.7.4

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY kafka_py_consumer.py ./
CMD [ "python3", "-u" ,"./kafka_py_consumer.py", "output_topic", "kafka:29092" ]
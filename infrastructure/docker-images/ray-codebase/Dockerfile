FROM python:3.7.4

WORKDIR /usr/src/app


COPY kafka_py.py ./kafka_py.py
COPY distributed_system ./distributed_system

COPY requirements.txt ./
RUN pip install -r requirements.txt

CMD ["python", "kafka_py.py", "-in", "lstm_input", "-out", "lstm_results", "-broker", "kafka:29092"]

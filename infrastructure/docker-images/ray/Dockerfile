FROM python:3.7.4

WORKDIR /usr/src/app

RUN apt-get -y update
RUN apt-get install -y --fix-missing \
    libxml2 \
    gcc \
    vim \
    iputils-ping \
    telnet \
    procps \
    && apt-get clean && rm -rf /tmp/* /var/tmp/*

COPY kafka_py.py ./kafka_py.py
COPY distributed_system ./distributed_system

COPY requirements.txt ./
RUN pip install -r requirements.txt


CMD ["echo", "Base Image Ready"]

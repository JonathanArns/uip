FROM python:3.7.4


RUN apt-get -y update
RUN apt-get install -y --fix-missing \
    libxml2 \
    gcc \
    vim \
    iputils-ping \
    telnet \
    procps \
    && apt-get clean && rm -rf /tmp/* /var/tmp/*

RUN pip install --upgrade pip
RUN pip install ray==0.7.6


CMD ["echo", "Base Image Ready"]

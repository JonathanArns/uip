FROM python:3.7.4

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install -r requirements.txt

CMD ["ray", "start", "--address=ray-master:3301"]
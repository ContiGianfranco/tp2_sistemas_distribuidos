FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika

COPY common /common/
COPY sentiment_adder /
ENTRYPOINT ["/bin/sh"]

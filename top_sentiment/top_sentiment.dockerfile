FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika
RUN pip3 install requests

COPY common /common/
COPY top_sentiment /
ENTRYPOINT ["/bin/sh"]

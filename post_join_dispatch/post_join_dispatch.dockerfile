FROM python:3.9.7-slim
RUN pip install --upgrade pip && pip3 install pika

COPY common /common/
COPY post_join_dispatch /
ENTRYPOINT ["/bin/sh"]

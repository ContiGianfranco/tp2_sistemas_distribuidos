FROM ubuntu:20.04

RUN apt update && apt install python3 python3-pip -y
RUN pip3 install pika

COPY common /common/
COPY joiner /
ENTRYPOINT ["/bin/sh"]
